import os
import shutil
import logging
import ruamel.yaml
from ruamel.yaml.comments import CommentedKeyMap, CommentedKeySeq
from ruamel.yaml.scalarstring import (
    SingleQuotedScalarString
    , DoubleQuotedScalarString
)

import stream_modifier
import shell

def is_tuple(value):
    return isinstance(value, tuple)

def is_sequence(value):
    return (
        isinstance(value, list)
        or isinstance(value, set)
        or isinstance(value, tuple)
        or isinstance(value, CommentedKeySeq)
    )

def is_mapping(value):
    return isinstance(value, dict) or isinstance(value, CommentedKeyMap)

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

def construct(cls, constructor, node):
    if isinstance(node, ruamel.yaml.ScalarNode):
        if node.style == "'":
            return cls(SingleQuotedScalarString(node.value, anchor=node.anchor))
        elif node.style == '"':
            return cls(DoubleQuotedScalarString(node.value, anchor=node.anchor))
        return cls(node.value)
    elif isinstance(node, ruamel.yaml.SequenceNode):
        # constructor._preserve_quotes = True
        try:
            return cls(*constructor.construct_sequence(node))
        except TypeError as e:
            raise RuntimeError(f"Cannot construct class '{cls.__name__}'") from e
    # constructor._preserve_quotes = True
    data = ruamel.yaml.CommentedMap()
    constructor.construct_mapping(node, maptyp=data, deep=True)
    data = {
        variable.replace("-", "_"): value
        for variable, value in data.items()
    }
    try:
        return cls(**data)
    except TypeError as e:
        raise RuntimeError(f"Cannot construct class '{cls.__name__}'") from e

# Hashable dictionary class
class HashableDict(dict):
    def __hash__(self) -> int:
        return id(self)

# Custom Exception class
class BuildError(RuntimeError):
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

class Path(Executable):
    yaml_tag = "!path"
    def __init__(self, *parts):
        self.parts = parts
    def __call__(self, scope, **_):
        parts = [execute(part, scope) for part in self.parts]
        parts = [
            part.replace('"', '') if isinstance(part, str) else part
            for part in parts
            if part is not None
        ]
        if parts:
            return os.path.normpath(os.path.join(*parts))
        return None
    @property
    def print_description(self):
        return False

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

class Python(Executable):
    yaml_tag = "!python"
    def __init__(self, code):
        self.code = code
    def __call__(self, scope, **_):
        return exec(self.code, {}, scope)  #pylint: disable=exec-used

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
        print("ok")
    @property
    def detail(self):
        return self.predicate

class Info(Executable):
    yaml_tag = "!info"
    def __init__(self, message):
        self.message = message
    def __call__(self, scope, **_):
        message = execute(self.message, scope, str_is_python=False)
        print(f"info      {message}")

class Important(Executable):
    yaml_tag = "!important"
    def __init__(self, message):
        self.message = message
    def __call__(self, scope, **_):
        message = str(execute(self.message, scope, str_is_python=False))
        line_length = max([len(line) for line in message.splitlines()])
        print("-" * line_length)
        print(message)
        print("-" * line_length)

class Warning(Executable):  # pylint: disable=redefined-builtin
    yaml_tag = "!warning"
    def __init__(self, message):
        self.message = message
    def __call__(self, scope, **_):
        return logging.warning(self.message.format_map(scope))

class Error(Executable):
    yaml_tag = "!error"
    def __init__(self, message):
        self.message = message
    def __call__(self, scope, **_):
        return logging.error(self.message.format_map(scope))

class Debug(Executable):
    yaml_tag = "!debug"
    def __init__(self, message):
        self.message = message
    def __call__(self, scope, **_):
        return logging.debug(self.message.format_map(scope))

class FileExists(Executable):
    yaml_tag = "!file.exists"
    def __init__(self, file):
        self.file = file
    def __call__(self, scope, **_):
        return os.path.isfile(execute(self.file, scope))
    @property
    def description(self):
        return self.file

class FileRemove(Executable):
    yaml_tag = "!file.remove"
    def __init__(self, file, must_exist=False):
        self.file       = file
        self.must_exist = must_exist
    def __call__(self, scope, **_):
        file = execute(self.file, scope)
        if os.path.isfile(file):
            print(f" - found file: {file}")
            os.remove(file)
            return True
        print(f" - file not found: {file}")
        if self.must_exist:
            raise BuildError("cannot remove non-existent file")
        return False
    @property
    def description(self):
        return self.file

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
            print(f" - found source: {source}")
            if self.make_dirs:
                destination_dir = os.path.dirname(destination)
                if destination_dir and not os.path.isdir(destination_dir):
                    print(f" - creating destination directory: {destination_dir}")
                    os.makedirs(destination_dir)
            try:
                print(f" - destination: {destination}")
                shutil.copyfile(source, destination)
                return True
            except Exception as e:
                raise BuildError("error during copying") from e
        elif self.must_exist:
            raise BuildError(f"cannot copy non-existent file: {source}")
        return False
    @property
    def description(self):
        return self.from_

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
    def description(self):
        return self.file

class Call(Executable):
    yaml_tag = "!call"
    def __init__(   # pylint: disable=redefined-builtin, redefined-outer-name
        self
        , program=None              # Path to executable
        , cwd=None                  # Directory from where to execute the program
        , args=None                 # List or dictionary of arguments
        , check=True                # Whether to check the return value of the application to be zero
        , print=True                # Whether to print stdout and stderr of the execution
        , on_success=None           # What to execute, when the return value is zero
        , on_fail=None              # What to execute, when the return value is non-zerp (set "check=False" first)
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
        self.on_success = on_success
        self.on_fail = on_fail
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

        options = {}
        options["cwd"] = os.path.abspath(execute(self.cwd, scope) or os.curdir)
        options["shell"] = True if execute(self.shell, scope) else False
        options["print_out"] = True if execute(self.print, scope) else False
        options["separate_process"] = True if execute(self.separate_process, scope) else False
        if log_file := execute(self.log_file, scope):
            options["print_file"] = open(log_file, "wb")

        print("command: ")
        prefix = "  "
        for part in command:
            print(prefix + part.strip())
            prefix = "    "
        print(f"working directory: {options['cwd']}")
        print("executing...")
        with stream_modifier.Prefix("  "):
            with stream_modifier.EliminateLogging():
                result = shell.shell(" ".join(command), **options)
        print(f"finished, took {result.duration:.2f}s")
        print(f"return code: {result.returncode}")

        if execute(self.check, scope):
            if result.returncode != 0:
                raise BuildError("call returned non-zero status code!")
        return result.returncode

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
                ))
        return result

class If(Executable):
    yaml_tag = "!if"
    def __init__(self, **arguments):
        self.if_    = arguments.get("when") or arguments.get("condition") or arguments["if"]
        self.then_  = arguments["then"]
        self.else_  = arguments.get("else", None)
    def __call__(self, scope, **args):
        if execute(self.if_, scope):
            return execute(self.then_, scope, dict_is_assignment=args["dict_is_assignment"])
        return execute(self.else_, scope, dict_is_assignment=args["dict_is_assignment"])
    @property
    def print_description(self):
        return False

class Set(Executable):
    yaml_tag = "!set"
    def __init__(self, **assignments):
        self.assignments = assignments
    def __call__(self, scope, **_):
        for variable, value in self.assignments.items():
            value       = execute(value, scope)
            variable    = execute(variable, scope, str_is_python=False)
            print(f"variable [{variable}] = {intelligent_repr(value)}")
            scope[variable] = value
    @property
    def print_indented(self):
        return False

yaml = ruamel.yaml.YAML()
yaml.preserve_quotes=True
yaml.register_class(Python)
yaml.register_class(Path)
yaml.register_class(Format)
yaml.register_class(Assert)
yaml.register_class(Info)
yaml.register_class(Important)
yaml.register_class(FileExists)
yaml.register_class(FileRemove)
yaml.register_class(FileCopy)
yaml.register_class(YamlRead)
yaml.register_class(YamlWrite)
yaml.register_class(Call)
yaml.register_class(For)
yaml.register_class(If)
yaml.register_class(Set)

def load_config(path):
    result = yaml.load(open(path, "rt", encoding="iso-8859-1").read())
    return result

class Scope:
    def __init__(self, variables=None, base=None):
        self.__variables = variables or {}
        self.__base = base
    def __getitem__(self, key):
        if key in self.__variables:
            return self.__variables[key]
        if isinstance(key, int):
            return self.__variables[self.__variables.keys()[key]]
        if self.__base:
            return self.__base[key]
        raise AttributeError
    def __setitem__(self, key, value):
        self.__variables[key] = value
    def __contains__(self, key):
        if key in self.__variables:
            return True
        if self.__base:
            return key in self.__base
        return False
    def __getattr__(self, key):
        if key in vars(Scope):
            return getattr(self, key)
        return self[key]
    def __len__(self):
        return len(self.__variables)
    def get(self, key, fallback=None):
        if key in self.__variables:
            return self.__variables[key]
        if self.__base:
            return self.__base[key]
        return fallback
    def to_dict(self):
        result = {}
        if self.__base:
            result.update(self.__base.items())
        result.update(self.__variables)
        return result
    def items(self):
        return self.to_dict().items()

def execute(
    value
    , scope
    , str_is_python=None
    , dict_is_assignment=False
    , deep=True
):
    if isinstance(value, Executable):
        if value.print_description:
            print(value.description)
        with stream_modifier.Prefix("  ") if value.print_indented else stream_modifier.Noop():
            result = value(
                scope
                , str_is_python=str_is_python
                , dict_is_assignment=dict_is_assignment
                , deep=deep
            )
            value = result
    elif isinstance(value, str):
        if isinstance(value, DoubleQuotedScalarString):
            str_is_python = False
        elif isinstance(value, SingleQuotedScalarString):
            str_is_python = True
        elif str_is_python is None:
            str_is_python = True
        if str_is_python is False:
            value = "f"+repr(value)
        return eval(value, {}, scope)  # pylint: disable=eval-used
    elif is_mapping(value) and deep:
        result = HashableDict()
        for variable, value in value.items():
            value       = execute(value, scope, str_is_python=str_is_python)
            variable    = execute(variable, scope, str_is_python=False)
            if dict_is_assignment:
                print(f"variable [{variable}] = {intelligent_repr(value)}")
                scope[variable] = value
            result[variable] = value
        value = result
    elif is_sequence(value) and deep:
        return [execute(val, scope, str_is_python=str_is_python) for val in value]
    elif is_tuple(value) and deep:
        return (execute(val, scope, str_is_python=str_is_python) for val in value)
    return value
