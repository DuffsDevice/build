import build_utils
import indenter
from functools import cmp_to_key

from build_utils import action_log, action_log_end

def build(
    config
    , targets
    , information=None
    , skip=None
):  # pylint: disable=redefined-outer-name, dangerous-default-value

    information = information or {}
    skip        = skip or []
    targets     = targets if build_utils.is_sequence(targets) else [targets]

    # Ensure all targets and subtarget skips exist
    for target in targets:
        if target not in config:
            raise build_utils.BuildError(
                f"Build target '{target} could not be found in the build config!"
            )
    for subtarget in skip:
        if subtarget not in config:
            raise build_utils.BuildError(
                f"Skip subtarget '{subtarget}' could not be found in the build config!"
            )

    with indenter.GlobalIndenter():

        # Determine full list of required subtargets
        visited_subtargets  = set(skip)
        subtargets          = []
        def add_dependencies(target):
            if target in visited_subtargets:
                return
            visited_subtargets.add(target)
            for subtarget in config[target].get("requires", []):
                add_dependencies(subtarget)
            subtargets.append(target)

        for target in targets:
            add_dependencies(target)

        # Sort list of subtargets from least depending to most depending
        def rank_subtargets(left, right):
            if right in config[left].get("requires", []):
                return 1
            if left in config[right].get("requires", []):
                return -1
            return 0
        subtargets.sort(key=cmp_to_key(rank_subtargets))

        # Check for proper order
        for i in range(len(subtargets)):  # pylint: disable=consider-using-enumerate
            for j in range(i, len(subtargets)):
                if rank_subtargets(subtargets[i], subtargets[j]) > 0:
                    raise build_utils.BuildError(
                        f"cyclic dependency involving subtargets "
                        f"'{subtargets[i]}' and '{subtargets[j]}' detected!"
                    )

        # Set some global information
        information["__subtargets"] = subtargets
        information["__targets"]    = targets

        # Set all available targets to None
        for possible_target in config:
            information[possible_target] = None

        # Create Scopes
        for index, subtarget in enumerate(subtargets):
            information[subtarget] = build_utils.Scope(
                {
                    "__name":       config[subtarget].get("name", subtarget)
                    , "__index":    index
                    , "__finished": False
                }
                , information
            )

        result = True
        for index, subtarget in enumerate(subtargets):
            with indenter.IndentationGuard(
                action_log("target" if subtarget in targets else "subtarget", subtarget)
                , action_log_end("target" if subtarget in targets else "subtarget", subtarget)
            ):
                # Run Sets
                for variable, value in config[subtarget].get("sets", {}).items():
                    information[variable] = build_utils.execute(value, information[subtarget])

                # Run Steps
                for steps in config[subtarget].get("steps", []):
                    for step in build_utils.ensure_sequence(steps):
                        try:
                            information[subtarget]["__result"]  = build_utils.execute(
                                step
                                , information[subtarget]
                                , dict_is_assignment=True
                            )
                            information[subtarget]["__success"] = True
                        except build_utils.BuildError as e:
                            information[subtarget]["__success"] = False
                            result = False
                            print(f" => build error: {e}")
                            break

                if not result:
                    break

        return result
