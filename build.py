import build_utils
import stream_modifier
import time
import threading
from concurrent import futures
from functools import cmp_to_key

def build(
    target_definitions_yaml_path
    , targets
    , scope=None
    , variants=None
    , skip_targets=None
    , from_targets=None
):
    # Preformat inputs
    if variants is None:
        variants    = [("default", {})]
    elif isinstance(variants, int):
        variants    = [(i, {}) for i in range(variants)]
    elif build_utils.is_sequence(variants):
        variants    = [(str(variant), {}) for variant in variants]
    else:
        assert build_utils.is_mapping(variants)
        variants    = [
            (str(variant), data)
            for variant, data in variants.items()
        ]

    scope           = scope or {}
    skip_targets    = skip_targets or []
    skip_targets    = skip_targets if build_utils.is_sequence(skip_targets) else [skip_targets]
    from_targets    = from_targets or []
    from_targets    = from_targets if build_utils.is_sequence(from_targets) else [from_targets]
    targets         = targets or []
    targets         = targets if build_utils.is_sequence(targets) else [targets]

    # Parse build yaml
    target_definitions = build_utils.load_config(target_definitions_yaml_path)

    # Ensure all targets and subtarget skips exist
    for target in targets:
        if target not in target_definitions:
            raise build_utils.BuildError(
                f"Build target '{target} could not be found in the build config!"
            )
    for subtarget in skip_targets:
        if subtarget not in target_definitions:
            raise build_utils.BuildError(
                f"Skip subtarget '{subtarget}' could not be found in the build config!"
            )
    for subtarget in from_targets:
        if subtarget not in target_definitions:
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
        for required_subtarget in target_definitions[subtarget].get("requires", []):
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
        if right in target_definitions[left].get("requires", []):
            return 1
        if left in target_definitions[right].get("requires", []):
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

    # Create all involved semaphores
    semaphores = set()
    for subtarget in subtargets:
        semaphores.update(target_definitions[subtarget].get("locks", []))
    for semaphore in semaphores:
        scope[semaphore] = threading.Semaphore()

    # Print Targets
    print("targets:")
    for target in targets:
        print(f" - {target}")
    print()

    # Print all Subtargets
    print("required subtargets:")
    for index, subtarget in enumerate(subtargets, 1):
        origins = subtarget_parents.get(subtarget, [])
        if len(origins) > 3:
            origins = [*origins[0:3], f"and {len(origins)} other (sub)targets"]
        print(
            f" {index: >2}|"
            + subtarget
            + (f" (required by {', '.join(origins)})" if origins else "")
        )
    print()

    # Print variants that will be built
    print("variants:")
    for variant, data in variants:
        print(f" - {build_utils.intelligent_repr(variant)}")
        if data:
            for variable, value in data.items():
                print(f"   - {variable}: {build_utils.intelligent_repr(value)}")
    print()

    # Convert variant data to scopes
    variants = [(variant, build_utils.Scope(data, scope)) for variant, data in variants]

    # Set some global information
    scope["__subtargets"] = subtargets
    scope["__targets"]    = targets
    scope["__variants"]   = build_utils.Scope({
        variant: scope
        for variant, scope in variants
    })

    # Run each variant in parallel
    print("executing")
    results = []
    with stream_modifier.Redirect(stream_modifier.STDERR, stream_modifier.STDOUT):
        with stream_modifier.Scope(stream_modifier.STDOUT):
            with futures.ThreadPoolExecutor(len(variants)) as pool:
                for variant, variant_scope in variants:
                    results.append(pool.submit(
                        build_variant
                        , variant
                        , variant_scope
                        , subtargets
                        , target_definitions
                    ))

            # Wait for all results to be completed
            futures.wait(results)
            results = [future.result() for future in results]
            result = all(results)

    return result

def build_variant(
    variant
    , scope
    , subtargets
    , target_definitions
):
    # Set all available targets to None
    for possible_target in target_definitions:
        scope[possible_target] = None

    # Create a scope for each subtarget
    for index, subtarget in enumerate(subtargets):
        scope[subtarget] = build_utils.Scope(
            {
                "__name":       target_definitions[subtarget].get("name", subtarget)
                , "__index":    index
                , "__finished": False
            }
            , scope
        )

    # Set variant information for this scope
    scope["__variant"] = variant

    with scope.get("__modifiers", stream_modifier.Noop):

        # Execute Subtargets
        result = True
        for index, subtarget in enumerate(subtargets, start=1):

            # Acquire potential semaphores
            for semaphore_name in target_definitions[subtarget].get("locks", []):
                semaphore = scope[semaphore_name]
                assert isinstance(semaphore, threading.Semaphore)
                semaphore.acquire()

            # Run the build step
            with stream_modifier.Prefix(f"{index}|"):
                print(subtarget)
                with stream_modifier.Indent():

                    # Run Sets
                    for variable, value in target_definitions[subtarget].get("sets", {}).items():
                        scope[variable] = build_utils.execute(value, scope[subtarget])

                    # Run Steps
                    for step in build_utils.ensure_sequence(
                        target_definitions[subtarget].get("steps", [])
                    ):
                        try:
                            scope[subtarget]["__result"]  = build_utils.execute(
                                step
                                , scope[subtarget]
                                , dict_is_assignment=True
                            )
                            scope[subtarget]["__success"] = True
                        except build_utils.BuildError as e:
                            scope[subtarget]["__success"] = False
                            result = False
                            print(f" => build error: {e}")
                            break

                    if result:
                        print("ok")
                    else:
                        print("finished with errors")
                        break

            # Release potential semaphores
            for semaphore_name in target_definitions[subtarget].get("locks", []):
                semaphore = scope[semaphore_name]
                assert isinstance(semaphore, threading.Semaphore)
                semaphore.release()

        if result:
            print("  ok")
        else:
            print("  finished with errors")

    return result