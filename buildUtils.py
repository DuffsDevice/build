import ruamel.yaml
import os
import sys
import shutil
import subprocess
import logging
import indenter
import shell
from ruamel.yaml.scalarstring import (
    SingleQuotedScalarString
    , DoubleQuotedScalarString
)

def construct(cls, constructor, node):
    if isinstance(node, ruamel.yaml.ScalarNode):
        if node.style == "'":
            return cls(SingleQuotedScalarString(node.value, anchor=node.anchor))
        elif node.style == '"':
            return cls(DoubleQuotedScalarString(node.value, anchor=node.anchor))
        return cls(node.value)
    elif isinstance(node, ruamel.yaml.SequenceNode):
        return constructor.construct_sequence()
    data = ruamel.yaml.CommentedMap()
    constructor.construct_mapping(node, maptyp=data, deep=True)
    try:
        return cls(**data)
    except TypeError as e:
        raise RuntimeError(f"Cannot construct class '{cls.__name__}'") from e

# Base Class
class Executable:
    yaml_tag = None
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
                attribute.strip("_"): value
                for attribute, value in vars(self).items()
            }
        )
    def __call__(self, _):
        raise RuntimeError(f"Executable Class '{type(self)}' has no implementation!")

class Path(Executable):
    yaml_tag = "!path"
    def __init__(self, value):
        self.value = value
    def __call__(self, scope):
        return os.path.normpath(execute(self.value, scope))

class Assert(Executable):
    yaml_tag = "!assert"
    def __init__(self, predicate):
        self.predicate = predicate
    def __call__(self, scope):
        if not execute(self.predicate, scope):
            raise RuntimeError(f"assertion failed: {self.predicate}")

class Info(Executable):
    yaml_tag = "!info"
    def __init__(self, message):
        self.message = message
    def __call__(self, scope):
        return logging.info(self.message.format_map(scope))

class Warning(Executable):
    yaml_tag = "!warning"
    def __init__(self, message):
        self.message = message
    def __call__(self, scope):
        return logging.warning(self.message.format_map(scope))

class Error(Executable):
    yaml_tag = "!error"
    def __init__(self, message):
        self.message = message
    def __call__(self, scope):
        return logging.error(self.message.format_map(scope))

class Debug(Executable):
    yaml_tag = "!debug"
    def __init__(self, message):
        self.message = message
    def __call__(self, scope):
        return logging.debug(self.message.format_map(scope))

class FileExists(Executable):
    yaml_tag = "!file.exists"
    def __init__(self, file):
        self.file = file
    def __call__(self, scope):
        return os.path.isfile(execute(self.file, scope))

class FileRemove(Executable):
    yaml_tag = "!file.remove"
    def __init__(self, file):
        self.file = file
    def __call__(self, scope):
        return os.remove(execute(self.file, scope))

class FileCopy(Executable):
    yaml_tag = "!file.copy"
    def __init__(self, **arguments):
        self.from_ = arguments["from"]
        self.to_ = arguments["to"]
    def __call__(self, scope):
        return shutil.copyfile(execute(self.from_, scope), execute(self.to_, scope))

class Shell(Executable):
    yaml_tag = "!shell"
    def __init__(self, program=None, cwd=None, args=None, check=True, display=False, success=None, explicit_shell=False):
        self.program = program
        self.cwd = cwd
        self.args = args
        self.check = check
        self.display = display
        self.success = success
        self.explicit_shell = explicit_shell
    def __call__(self, scope):
        command     = [execute(self.program, scope)]
        arguments   = self.args
        if not isinstance(self.args, dict) and not isinstance(self.args, list):
            arguments = execute(self.args, scope)
        if isinstance(arguments, dict):
            arguments = [{parameter: value} for parameter, value in arguments.items()]
        elif not isinstance(arguments, list):
            arguments = [arguments]
        actual_arguments = []
        for argument in arguments:
            if isinstance(argument, dict):
                assert len(argument) == 1
                parameter, value = next(iter(argument.items()))
                if not parameter:
                    argument = value
            if isinstance(argument, list):
                actual_arguments.extend(argument)
            else:
                actual_arguments.append(argument)
        for argument in actual_arguments:
            if isinstance(argument, dict):
                assert len(argument) == 1
                parameter, value = next(iter(argument.items()))
                parameter   = execute(parameter, scope, str_is_python=False)
                value       = execute(value, scope)
                argument = f"--{parameter}"
                if value is None:
                    value = []
                elif not isinstance(value, list):
                    value = [value]
                for val in value:
                    val = str(val)
                    argument += f' "{val}"' if val.count(" ") else f" {val}"
                command.append(argument)
            else:
                argument = execute(argument, scope)
                argument = str(argument)
                argument = f'"{argument}"' if argument.count(" ") else f" {argument}"
                command.append(argument)
        options = {}
        options["cwd"] = execute(self.cwd, scope)
        options["shell"] = True if execute(self.explicit_shell, scope) else False
        print(f"shell [{self.program}]...")
        with indenter.IndentationGuard():
            result = shell.shell(" ".join(command), **options)
        print(f" => shell [{self.program}] finished with return code {result.returncode}")
        if execute(self.check, scope):
            if result.returncode != 0:
                raise RuntimeError("Shell call returned non-zero status code!")
        return result.returncode

class For(Executable):
    yaml_tag = "!for"
    def __init__(self, **arguments):
        self.for_ = arguments.get("for")
        self.in_ = arguments.get("in")
        self.do_ = arguments.get("do")
        self.flatten = arguments.get("flatten", False)
    def __call__(self, scope):
        iterable = execute(self.for_, scope)
        result = []
        for element in iterable:
            if isinstance(self.in_, list):
                for variable, part in zip(self.in_, element):
                    scope[variable] = part
            else:
                scope[variable] = element
            if isinstance(self.do_, list):
                results = []
                for statement in self.do_:
                    results.append(execute(statement, scope))
                if self.flatten:
                    result.extend(results)
                else:
                    result.append(results)
            else:
                result += execute(self.do_, scope)
        return result

class If(Executable):
    yaml_tag = "!if"
    def __init__(self, **arguments):
        self.if_ = arguments["if"]
        self.then_ = arguments["then"]
        self.else_ = arguments.get("else", None)
    def __call__(self, scope):
        if execute(self.if_, scope):
            return execute(self.then_, scope)
        return execute(self.else_, scope)

class Set(Executable):
    yaml_tag = "!set"
    def __init__(self, **assignments):
        self.assignments = assignments
    def __call__(self, scope):
        for variable, value in self.assignments.items():
            value = execute(value, scope)
            variable = execute(variable, scope, str_is_python=False)
            print(f"variable [{variable}] = {value}")
            scope[variable] = value

yaml = ruamel.yaml.YAML()
yaml.register_class(Path)
yaml.register_class(Assert)
yaml.register_class(Info)
yaml.register_class(FileExists)
yaml.register_class(FileRemove)
yaml.register_class(FileCopy)
yaml.register_class(Shell)
yaml.register_class(For)
yaml.register_class(If)
yaml.register_class(Set)
yaml.preserve_quotes=True

def load_config(path):
    return yaml.load(open(path, "rt", encoding="iso-8859-1").read())

class Scope:
    def __init__(self, variables=None, base=None):
        self.__variables = variables or {}
        self.__base = base
    def __getitem__(self, key):
        if key in self.__variables:
            return self.__variables[key]
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
        if key in ["__variables", "__base"]:
            return getattr(self, key)
        return self[key]

def execute(value, scope, str_is_python=True, deep=True):
    if isinstance(value, Executable):
        return value.__call__(scope)
    elif isinstance(value, str):
        if isinstance(value, DoubleQuotedScalarString):
            str_is_python = False
        elif isinstance(value, SingleQuotedScalarString):
            str_is_python = True
        if not str_is_python:
            value = "f"+repr(value)
        return eval(value, {}, scope)
    elif value is None:
        return value
    elif isinstance(value, bool):
        return value
    elif isinstance(value, dict) and deep:
        return {
            execute(attr, scope, str_is_python=False)
            : execute(val, scope)
            for attr, val in value.items()
        }
    elif isinstance(value, list) and deep:
        return [execute(val, scope) for val in value]
    else:
        raise RuntimeError(f"Unsupported statement of type '{type(value)}'")
    return value