import build_utils
import stream_modifier
from functools import cmp_to_key

from build_utils import action_log, action_log_end

def build(
    config
    , targets
    , information=None
    , skip_targets=None
    , from_targets=None
):  # pylint: disable=redefined-outer-name, dangerous-default-value

    information     = information or {}
    skip_targets    = skip_targets or []
    from_targets    = from_targets or []
    targets         = targets if build_utils.is_sequence(targets) else [targets]

    # Ensure all targets and subtarget skips exist
    for target in targets:
        if target not in config:
            raise build_utils.BuildError(
                f"Build target '{target} could not be found in the build config!"
            )
    for subtarget in skip_targets:
        if subtarget not in config:
            raise build_utils.BuildError(
                f"Skip subtarget '{subtarget}' could not be found in the build config!"
            )
    for subtarget in from_targets:
        if subtarget not in config:
            raise build_utils.BuildError(
                f"From subtarget '{subtarget}' could not be found in the build config!"
            )

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
        for required_subtarget in config[subtarget].get("requires", []):
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
        if right in config[left].get("requires", []):
            return 1
        if left in config[right].get("requires", []):
            return -1
        return 0
    subtargets.sort(key=cmp_to_key(rank_subtargets))

    # Check for proper order
    for i in range(num_subtargets):  # pylint: disable=consider-using-enumerate
        for j in range(i, num_subtargets):
            if rank_subtargets(subtargets[i], subtargets[j]) > 0:
                raise build_utils.BuildError(
                    f"cyclic dependency involving subtargets "
                    f"'{subtargets[i]}' and '{subtargets[j]}' detected!"
                )

    print("targets:")
    for target in targets:
        print(f" - {target}")
    print("required subtargets:")
    for index, subtarget in enumerate(subtargets, 1):
        origins = subtarget_parents.get(subtarget, [])
        if len(origins) > 3:
            origins = [*origins[0:3], f"and {len(origins)} other (sub)targets"]
        print(
            f" {index: >2}. "
            + (f"=> {subtarget}" if subtarget in target else subtarget)
            + (f" (required by {', '.join(origins)})" if origins else "")
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

    with stream_modifier.StreamModificationScope(stream_modifier.STDOUT, stream_modifier.STDERR):

        # Execute Subtargets
        result = True
        for index, subtarget in enumerate(subtargets):
            prefix = f"({index+1}/{num_subtargets}) "
            prefix += "target" if subtarget in targets else "subtarget"
            with stream_modifier.Indent():
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
