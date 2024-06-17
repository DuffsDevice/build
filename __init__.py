import os
import time
import traceback
import argparse
import logging
from typing import Dict
from concurrent import futures
from functools import cmp_to_key
from difflib import SequenceMatcher
from ruamel.yaml.scalarstring import (
    SingleQuotedScalarString
    , DoubleQuotedScalarString
)

from build import utils
from build import stream_modifier
from build import hierarchical_threads  # pylint: disable=unused-import

def ensure_name_in_list(names, name, scope:Dict|utils.Scope=None, noun="name", exception=True) -> bool:
    """
    Checks, whether 'name' is in the supplied list 'names'.
    If not, it suggests alternatives with 'noun' as the noun used for the message printed.
    If 'exception' is False, prints the error message instead of raising an exception.
    Supplying 'scope' allows oneself to override the printing method.
    """
    if name in names:
        return True

    # Determine likelihood of match for each option
    matches = [
        (SequenceMatcher(None, name, potential_match).ratio(), potential_match)
        for potential_match in names
        if isinstance(potential_match, str)
    ]
    matches.sort(reverse=True)
    message = f"{noun} '{name}' could not be found in the build config!"

    # Give suggestion?
    if matches and matches[0][0] > 0.3:
        message += f" Did you mean '{matches[0][1]}'"

        # Give two suggestions, if the first and second one have similar likelihood
        if len(matches) >= 2 and matches[0][0] < 0.85 and matches[1][0] > 0.4:
            message += f", or '{matches[1][1]}'"

        message += "?"

    # Print message or raise an exception
    if exception:
        raise utils.BuildError(message)
    else:
        utils.print_error(scope, message)

    return False

def argparse_argument_option(argument, scope={}):
    if isinstance(argument, SingleQuotedScalarString):
        argument = utils.execute(argument, scope)
    elif not isinstance(argument, DoubleQuotedScalarString):
        assert " " not in argument
        argument = "--" + argument
    return argument

def argparse_argument_identifier(argument, scope={}):
    argument = argparse_argument_option(argument, scope)
    return argument.lstrip("-").replace("-", "_")

class Build:
    def __init__(self, yaml_path: str) -> None:
        self.config     = utils.load_config(yaml_path)
        self.targets    = self.config.get("targets") or {}
        self.arguments  = self.config.get("arguments") or {}
        self.ressources = self.config.get("ressources") or {}
        self.scope      = {}

        assert utils.is_mapping(self.arguments)

        # Determine list of semaphore names
        for subtarget in self.targets:
            for ressource in self.targets[subtarget].get("locks", []):
                self.ressources.setdefault(ressource, {})
        for ressource, data in self.ressources.items():
            data.setdefault("per-variant", False)
            data.setdefault("count", 1)

    def set_vars(self, variables:Dict|None = None, **kwargs):
        if variables is not None:
            self.scope |= variables
        self.scope |= kwargs

    def set_var(self, variable, *value):
        if value:
            assert len(value) == 1
            self.scope[variable] = value[0]
        elif variable in self.scope:
            del self.scope[variable]

    def set_print(self, *value):
        self.set_var("_print", *value)

    def set_print_debug(self, *value):
        self.set_var("_print.debug", *value)

    def set_print_info(self, *value):
        self.set_var("_print.info", *value)

    def set_print_warning(self, *value):
        self.set_var("_print.warning", *value)

    def set_print_error(self, *value):
        self.set_var("_print.error", *value)

    def set_print_critical(self, *value):
        self.set_var("_print.critical", *value)

    def set_print_fatal(self, *value):
        self.set_var("_print.fatal", *value)

    def set_print_using_logging(self):
        self.set_var("_print.debug", logging.debug)
        self.set_var("_print.info", logging.info)
        self.set_var("_print.warning", logging.warning)
        self.set_var("_print.error", logging.error)
        self.set_var("_print.critical", logging.critical)
        self.set_var("_print.fatal", logging.fatal)

    def amend_argparse(self, parser:argparse.ArgumentParser):
        """
        Function that adds all build-specific arguments to the supplied ArgumentParser instance.
        Optionally, a dictionary can be supplied with information that should be available when adding
        the options.
        """
        for argument, options in self.arguments.items():

            # Determine the name of the option
            argument = argparse_argument_option(argument, self.scope)

            # Evaluate the options to pass to argparse
            options = utils.execute(options, self.scope)

            # Format list of help lines into one string
            if utils.is_sequence(options.get("help")):
                options["help"] = "\n - ".join(options["help"])

            # Add the argument
            parser.add_argument(argument, **options)

    def try_build(self, *args, **kwargs):
        """
        Executes self.build, but within a try-except block that will pretty-print an exception,
        if it is thrown by the build.
        """
        try:
            return self.build(*args, **kwargs)
        except utils.BuildError as cause:
            utils.print_info(self.scope)
            utils.print_info(self.scope, "exception:")
            while cause:
                message         = getattr(cause, "message", str(cause))
                cause_type      = type(cause).__name__
                cause_origin    = getattr(cause, "__traceback__", None)
                cause           = getattr(cause, "__cause__", None)
                if cause is None:
                    utils.print_info(self.scope, f" > [{cause_type}] {message}")
                else:
                    utils.print_info(self.scope, f" - [{cause_type}] {message}:")
                if cause_origin is not None:
                    summary = traceback.extract_tb(cause_origin)
                    while summary:
                        entry = summary.pop(0)
                        if summary:
                            utils.print_info(
                                self.scope
                                , f"    - {os.path.basename(entry.filename)}:{entry.lineno}"
                            )
                        else:
                            utils.print_info(
                                self.scope
                                , f"    > {os.path.basename(entry.filename)}:{entry.lineno}"
                            )

        return False

    def determine_subtargets(
        self
        , targets
        , skip_targets=None
        , from_targets=None
    ) -> Dict[str, str]:
        """
        Determines the list of subtargets from
        - the list of targets,
        - a list of subtargets to start the build "from" and
        - a list of subtargets to skip.
        Returns a dictionary in the correct order of thr form
        "{subtarget: [list of targets, that require this subtarget]}"
        """

        skip_targets    = skip_targets or []
        skip_targets    = skip_targets if utils.is_sequence(skip_targets) else [skip_targets]
        from_targets    = from_targets or []
        from_targets    = from_targets if utils.is_sequence(from_targets) else [from_targets]
        targets         = targets or []
        targets         = targets if utils.is_sequence(targets) else [targets]

        # Ensure all targets and subtarget skips exist
        for target in targets:
            ensure_name_in_list(self.targets, target, noun="build target", scope=self.scope)
        for subtarget in skip_targets:
            ensure_name_in_list(self.targets, subtarget, noun="skip subtarget", scope=self.scope)
        for subtarget in from_targets:
            ensure_name_in_list(self.targets, subtarget, noun="from subtarget", scope=self.scope)

        # Determine full list of required subtargets
        visited_subtargets  = set(skip_targets)
        subtargets          = []
        subtarget_parents   = {}
        subtarget_children  = {}

        def resolve_required_subtargets(subtarget, required_by=None):
            if required_by:
                subtarget_parents.setdefault(subtarget, []).append(required_by)
            if subtarget in visited_subtargets:
                return
            visited_subtargets.add(subtarget)
            for required_subtarget in self.targets[subtarget].get("subtargets", []):
                resolve_required_subtargets(required_subtarget, subtarget)
            subtargets.append(subtarget)

        for target in targets:
            resolve_required_subtargets(target)
            if target not in subtargets:
                subtargets.append(target)
        num_subtargets = len(subtargets)

        # If "from" is set, determine impact set
        if from_targets:
            backlog = from_targets.copy()
            while backlog:
                subtarget = backlog.pop()
                for origin_target in subtarget_parents.get(subtarget, []):
                    subtarget_children.setdefault(origin_target, []).append(subtarget)
                    if origin_target not in from_targets:
                        from_targets.append(origin_target)
                        backlog.append(origin_target)
            subtargets = [subtarget for subtarget in subtargets if subtarget in from_targets]

        # Sort list of subtargets from least depending to most depending
        def rank_subtargets(left, right):
            if right in self.targets[left].get("subtargets", []):
                return 1
            if left in self.targets[right].get("subtargets", []):
                return -1
            return 0
        subtargets.sort(key=cmp_to_key(rank_subtargets))

        # Check for proper order
        for i in range(num_subtargets):  # pylint: disable=consider-using-enumerate
            for j in range(i, num_subtargets):
                if rank_subtargets(subtargets[i], subtargets[j]) > 0:
                    raise utils.BuildError(
                        f"cyclic dependency involving subtargets "
                        f"'{subtargets[i]}' and '{subtargets[j]}' detected!"
                    )

        return {subtarget: subtarget_parents.get(subtarget, []) for subtarget in subtargets}

    def check_arguments(self, arguments, subtargets, scope):
        for subtarget in subtargets:
            required_arguments = self.targets[subtarget].get("required-arguments", [])
            for argument in required_arguments:

                condition           = None
                condition_message   = None
                if isinstance(argument, dict):
                    assert len(argument) == 1
                    argument, condition = next(iter(argument.items()))
                if isinstance(condition, dict):
                    condition           = condition["condition"]
                    condition_message   = condition.get("error-message")

                # Determine the command line option name as well as the identifier used
                argument_option     = argparse_argument_option(argument, scope)
                argument_identifier = argparse_argument_identifier(argument, scope)

                # Make sure, the argument is defined
                if argument not in self.arguments:
                    raise utils.BuildError(
                        f"argument '{argument_option}' is required by target '{subtarget}'"
                        f", but is not specified in the build yaml under 'arguments'"
                    )

                # Make sure the argument was supplied
                if argument_identifier not in arguments:
                    raise utils.BuildError(
                        f"argument '{argument_option}' is required by target '{subtarget}'"
                        f", but was not in the parsed arguments as '{argument_identifier}'"
                    )

                # Make sure the arguments value is valid
                if condition is None:
                    if arguments[argument_identifier] is None:
                        raise utils.BuildError(
                            f"missing argument '{argument_option}', required by target '{subtarget}'"
                        )
                else:
                    exception_message = f"argument '{argument_option}', required by target '{subtarget}' is not valid"
                    try:
                        condition_met = utils.execute(condition, utils.Scope(arguments, scope))
                        if not condition_met:
                            raise utils.BuildError(
                                f"{exception_message}: "
                                + (condition_message or "the condition '{condition}' was not met")
                            )
                    except utils.BuildError as error:
                        if condition_message:
                            raise utils.BuildError(f"{exception_message}: " + condition_message) from error
                        raise utils.BuildError(exception_message) from error

    def build(
        self
        , targets
        , arguments:dict|argparse.Namespace|None=None
        , variants=None
        , skip_targets=None
        , from_targets=None
        , variants_in_parallel=False
        , subtargets_in_parallel=False
        , fail_early=True
    ):
        # Preformat inputs
        if variants is None:
            variants    = [("default", {})]
        elif isinstance(variants, int):
            variants    = [(i, {}) for i in range(variants)]
        elif utils.is_sequence(variants):
            variants    = [(str(variant), {}) for variant in variants]
        else:
            assert utils.is_mapping(variants)
            variants    = [
                (str(variant), data)
                for variant, data in variants.items()
            ]
        scope       = utils.Scope({}, self.scope)
        if isinstance(arguments, dict):
            arguments = utils.Scope(arguments)

        # Determine subtargets
        subtargets  = self.determine_subtargets(targets, skip_targets, from_targets)

        # Ensure, all arguments required by all subtargets are available
        self.check_arguments(arguments, subtargets, scope)

        # Print Targets
        utils.print_info(self.scope, "targets:")
        for target in targets:
            utils.print_info(self.scope, f" - {target}")
        utils.print_info(self.scope)

        # Print all Subtargets
        utils.print_info(self.scope, "subtargets:")
        for index, subtarget in enumerate(subtargets, 1):
            origins = subtargets.get(subtarget, [])
            if len(origins) > 3:
                origins = [*origins[0:3], f"and {len(origins)} other (sub)targets"]
            utils.print_info(self.scope,
                f" {index: >2}|"
                + subtarget
                + (f" (required by {', '.join(origins)})" if origins else "")
            )
        utils.print_info(self.scope)

        # Print variants that will be built
        utils.print_info(self.scope, "variants:")
        for variant, data in variants:
            utils.print_info(self.scope, f" - {utils.intelligent_repr(variant)}")
            if data:
                for variable, value in data.items():
                    utils.print_info(self.scope, f"   - {variable}: {utils.intelligent_repr(value)}")
        utils.print_info(self.scope)

        # Convert variant data to scopes
        variants = [(variant, utils.Scope(data, scope)) for variant, data in variants]

        # Create all involved semaphores
        for ressource, data in self.ressources.items():
            data.setdefault("per-variant", False)
            data.setdefault("count", 1)
            if data["per-variant"]:
                scope[ressource] = utils.Semaphore(data["count"])
            else:
                for variant in variants:
                    variant[1][ressource] = utils.Semaphore(data["count"])

        # Print Ressources
        if self.ressources:
            utils.print_info(self.scope, "ressources:")
            for ressource, data in self.ressources.items():
                utils.print_info(self.scope,
                    f" - {ressource} (type: "
                    + ("per-variant" if data["per-variant"] else "global")
                    + ", count: "
                    + str(data["count"])
                    + ")"
                )
            utils.print_info(self.scope)

        # Set some global information
        scope["arguments"]     = arguments
        scope["_subtargets"]   = subtargets
        scope["_targets"]      = targets
        scope["_variants"]     = utils.Scope({
            variant: scope
            for variant, scope in variants
        })

        # Build all variants
        utils.print_info(self.scope, "executing:")
        with stream_modifier.Scope(
            stream_modifier.Target("scope['_print.debug']", scope=self.scope)
            , stream_modifier.Target("scope['_print.info']", scope=self.scope)
            , stream_modifier.Target("scope['_print.warning']", scope=self.scope)
            , stream_modifier.Target("scope['_print.error']", scope=self.scope)
            , stream_modifier.Target("scope['_print.critical']", scope=self.scope)
            , stream_modifier.Target("scope['_print.fatal']", scope=self.scope)
        ):
            result = self.build_variants(
                variants
                , scope
                , subtargets
                , variants_in_parallel
                , subtargets_in_parallel
                , fail_early
            )

        # Give summary
        utils.print_info(self.scope)
        utils.print_info(self.scope, "summary:")
        for variant, data in variants:
            utils.print_info(self.scope, f" - {utils.intelligent_repr(variant)}")
            for index, subtarget in enumerate(subtargets, 1):
                if data[subtarget]["_finished"]:
                    delta_start = data[subtarget]["_start_time"] - data["_start_time"]
                    utils.print_info(self.scope,
                        f" {index: >2}|{subtarget}"
                        + f" (started after: {delta_start:.2f}s, took {data[subtarget]['_duration']:.2f}s)"
                    )
                else:
                    utils.print_info(self.scope, f" {index: >2}|{subtarget}")

        return result

    def build_variants(
        self
        , variants
        , scope
        , subtargets
        , variants_in_parallel
        , subtargets_in_parallel
        , fail_early
    ):
        scope["_start_time"]   = time.time()
        scope["_finished"]     = False

        # Run each variant in parallel
        result  = True
        if variants_in_parallel:
            results = []
            with futures.ThreadPoolExecutor(len(variants)) as pool:
                for variant, variant_scope in variants:
                    results.append(pool.submit(
                        self.build_variant
                        , variant
                        , variant_scope
                        , subtargets
                        , subtargets_in_parallel
                        , fail_early
                    ))
            futures.wait(
                results
                , return_when=futures.FIRST_EXCEPTION if fail_early else futures.ALL_COMPLETED
            )
            for future, variant in zip(results, variants):
                if future.exception():
                    raise utils.BuildError(
                        f"variant {utils.intelligent_repr(variant[0])} finished with errors"
                    ) from future.exception()
                future.cancel()
                if not future.done or not future.result():
                    result = False
        else:
            for variant, variant_scope in variants:
                if not self.build_variant(
                    variant
                    , variant_scope
                    , subtargets
                    , subtargets_in_parallel
                    , fail_early
                ):
                    result = False
                    break

        scope["_duration"]  = time.time() - scope["_start_time"]
        scope["_finished"]  = True
        scope["_success"]   = result

        return result

    def build_variant(
        self
        , variant
        , scope
        , subtargets
        , subtargets_in_parallel
        , fail_early
    ):
        # Set all available targets to None
        for possible_target in self.targets:
            scope[possible_target] = None

        # Create a scope for each subtarget
        for index, subtarget in enumerate(subtargets, start=1):
            scope[subtarget] = utils.Scope(
                {
                    "_name":           self.targets[subtarget].get("name", subtarget)
                    , "_index":        index
                    , "_finished":     False
                    , "_success":      None
                    , "_exception":    None
                }
                , scope
            )

        # Set variant information for this scope
        scope["_variant"] = variant
        scope["_finished"] = False
        scope["_success"] = None
        scope["_exception"] = None

        # Execute Subtargets
        result = True
        with scope.get("_modifiers", stream_modifier.Noop):
            if subtargets_in_parallel:
                results = []
                with futures.ThreadPoolExecutor(len(subtargets)) as pool:
                    for index, subtarget in enumerate(subtargets, start=1):
                        results.append(pool.submit(
                            self.build_subtarget
                            , subtarget
                            , index
                            , scope
                        ))
                futures.wait(
                    results
                    , return_when=futures.FIRST_EXCEPTION if fail_early else futures.ALL_COMPLETED
                )
                for future, subtarget in zip(results, subtargets):
                    if future.exception():
                        raise utils.BuildError(
                            f"subtarget '{subtarget}' finished with errors"
                        ) from future.exception()
                    future.cancel()
                    if not future.done or not future.result():
                        result = False
            else:
                for index, subtarget in enumerate(subtargets, start=1):
                    try:
                        if not self.build_subtarget(subtarget, index, scope):
                            result = False
                            break
                    except utils.BuildError as e:
                        scope["_finished"]     = False
                        scope["_success"]      = None
                        scope["_exception"]    = e
                        result = False
                        raise

            if result:
                utils.print_info(self.scope, "  ok")
            else:
                utils.print_info(self.scope, "  finished with errors")

        return result


    def build_subtarget(
        self
        , subtarget
        , index
        , scope
    ):
        # Wait for all dependencies of this subtarget to finish
        for dependency in self.targets[subtarget].get("subtargets", []):
            while not scope[dependency]["_finished"]:
                if scope[dependency]["_success"] is False:
                    return
                time.sleep(0.1)

        # Acquire potential semaphores
        for require in self.targets[subtarget].get("locks", []):
            if isinstance(require, dict):
                assert len(require) == 1
                semaphore_name, semaphore_count = next(iter(require.items()))
            else:
                semaphore_name, semaphore_count = require, 1
            semaphore = scope[subtarget][semaphore_name]
            assert isinstance(semaphore, utils.Semaphore)
            semaphore.acquire(semaphore_count)

        # Run the build step
        scope[subtarget]["_start_time"]  = time.time()
        with stream_modifier.Prefix(f"{index}|"):
            utils.print_info(self.scope, subtarget)
            with stream_modifier.Indent():

                # Run Steps
                try:
                    result = utils.execute(
                        utils.ensure_sequence(self.targets[subtarget].get("steps", []))
                        , scope[subtarget]
                        , dict_is_assignment=True
                        , print_description=True
                    )
                    scope[subtarget]["_result"] = result[-1] if result else None
                except utils.BuildError as e:
                    scope[subtarget]["_success"]   = False
                    scope[subtarget]["_exception"] = e
                    scope[subtarget]["_duration"]  = time.time() - scope[subtarget]["_start_time"]
                    raise utils.BuildError(f"target '{subtarget}' finished with errors") from e

                # Run Sets
                for variable, value in self.targets[subtarget].get("sets", {}).items():
                    scope[variable] = utils.execute(value, scope[subtarget])
                    utils.print_info(self.scope, f"variable [{variable}] = {utils.intelligent_repr(scope[variable])}")

                utils.print_info(self.scope, "ok")

        # Set "finished" to true
        scope[subtarget]["_success"]   = True
        scope[subtarget]["_finished"]  = True
        scope[subtarget]["_duration"]  = time.time() - scope[subtarget]["_start_time"]

        # Release potential semaphores
        for require in self.targets[subtarget].get("locks", []):
            if isinstance(require, dict):
                assert len(require) == 1
                semaphore_name, semaphore_count = next(iter(require.items()))
            else:
                semaphore_name, semaphore_count = require, 1
            semaphore = scope[subtarget][semaphore_name]
            assert isinstance(semaphore, utils.Semaphore)
            semaphore.release(semaphore_count)

        return result