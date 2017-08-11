# distutils: language = c++
# cython: embedsignature=True

import inspect
from multiprocessing import Pool
from collections import defaultdict, namedtuple

class CallNode:
    """CallNode objects wrap a function for use in a CallGraph.

    :param function: The function (or callable object) to be called when the output is needed or an input changes
    :param output: A name (or list of names if the function returns a tuple) to bind the function output to. If not given, defaults to the name of the function
    :param async: True if the function can be called in a background process
    :param remap: A dictionary mapping function parameter names to scope names
    :param as_needed: True if the function should not be called when its inputs change, but only as something that needs its value is called
    """
    def __init__(self, function, output=None,
                 async=False, remap={}, as_needed=False):
        # set self.outputs,dependencies for given
        # module
        if output is None:
            output = function.__name__
        if not isinstance(output, list):
            output = [output]

        sig = inspect.signature(function)
        dependencies = [name for name in sig.parameters]

        self.function = function
        self.outputs = output
        self.dependencies = dependencies
        self.async = async
        self.remap = dict(remap)
        self.as_needed = as_needed

    def __repr__(self):
        return 'CallNode({}, output={}, async={}, remap={}, as_needed={})'.format(self.function, self.outputs, self.async, self.remap, self.as_needed)

CallGraphState = namedtuple('CallGraphState', ['scope', 'dirty_inputs', 'dirty_outputs', 'dirty_as_needed'])

class CallGraph:
    """Handles the reactivity for a set of :py:class:`CallNode` objects.

    Each `CallNode` has a set of input (dependency) and output
    names. Nodes are added to the graph via
    :py:meth:`peact.CallGraph.register`.

    """
    def __init__(self):
        self.modules = []
        self.moduleLists = []
        # map module -> modules it depends on
        self.deps = defaultdict(list)
        # map module -> modules that depend on it
        self.revdeps = defaultdict(list)
        # map property name -> farthest module that provides the property and all of its (transient) revdeps
        self.rollingOutputRevdeps = defaultdict(set)
        # map property name -> modules that depend on it as inputs, even transiently
        self.rollingRevdeps = defaultdict(set)
        self.pool = None

        self.state = CallGraphState({}, set(), set(), set())
        self.pumping = None
        self.printedExceptions = set()

    @property
    def scope(self):
        return self.state.scope

    def register(self, function, *args, **kwargs):
        """Register a function as part of this graph. Takes the same
        parameters as :py:class:`peact.CallNode`.

        :return: The given function
        """
        node = CallNode(function, *args, **kwargs)
        self.moduleLists.append([node])
        return function

    def register_last(self, function, *args, **kwargs):
        """Register a function as part of this graph, after the last function
        that supplies any quantity of the same name. Takes the same
        parameters as :py:class:`peact.CallNode`.

        :return: The given function

        """
        node = CallNode(function, *args, **kwargs)
        node_names = set(node.outputs)

        for (i, list_) in enumerate(self.moduleLists[::-1]):
            if any(node_names.intersection(other.outputs) for other in list_):
                self.moduleLists.insert(-i, [node])
                break

        return function

    def register_deferred(self, target):
        """Registers a list object. This list should contain
        :py:class:`peact.CallNode` objects and will be consulted
        dynamically every time :py:meth:`peact.CallGraph.rebuild` is
        called.

        :param target: List object containing :py:class:`peact.CallNode` objects
        """
        self.moduleLists.append(target)
        return target

    def unregister(self, function, rebuild=True):
        """Remove the given function from the call graph.

        :param function: The function which should be removed
        :param rebuild: If True, immediately rebuild the call graph
        """
        try:
            index = [i for (i, mod) in enumerate(self.moduleLists)
                     if len(mod) == 1 and
                     (function == mod or mod[0].function == function)][0]
            del self.moduleLists[index]
        except IndexError:
            pass
        if rebuild:
            self.rebuild()

    def unregister_deferred(self, target, rebuild=True):
        """Remove the given dynamic `CallNode` provider from the graph.

        :param target: The list object which should be removed
        :param rebuild: If True, immediately rebuild the call graph
        """

        try:
            index = [i for (i, mod) in enumerate(self.moduleLists)
                     if mod is target][0]
            del self.moduleLists[index]
        except IndexError:
            pass
        if rebuild:
            self.rebuild()

    def clear(self):
        """Remove all modules from the call graph"""
        self.moduleLists = []
        self.rebuild()

    def rebuild(self, mark_dirty=True):
        """Build the dependency graph for all modules currently in the graph,
        as well as data structures for efficient dispatch of data.

        :param mark_dirty: If True, mark all properties in the graph as needing a recomputation
        """
        modules = []

        for mods in self.moduleLists:
            modules.extend(mods)

        # build the dependency graph
        # CallNode -> [CallNode]
        deps = defaultdict(list)
        revdeps = defaultdict(list)
        # opendeps[name] is a list of modules which depend on name
        # when nothing which provides name has been added to the graph
        # previously; these should be notified when name is injected
        # str -> [CallNode]
        opendeps = defaultdict(list)
        # providers is a mapping of quantity name to the last module
        # that provided it in the graph
        # str -> CallNode
        providers = {}

        for mod in modules:
            for dep in [mod.remap.get(d, d) for d in mod.dependencies]:
                if dep in providers:
                    dep_mod = providers[dep]
                    deps[mod].append(dep_mod)
                    revdeps[dep_mod].append(mod)
                else:
                    opendeps[dep].append(mod)
            for out in mod.outputs:
                providers[out] = mod

        # now create the cumulative sets of modules that will be
        # recomputed based on transient dependencies

        # CallNode -> {CallNode}
        rollingRevdeps = defaultdict(set)
        rollingOutputRevdeps = defaultdict(set)

        for mod in modules[::-1]:
            mod_rolling_revdeps = set()
            for revdep in revdeps[mod]:
                mod_rolling_revdeps.add(revdep)
                mod_rolling_revdeps.update(rollingRevdeps[revdep])
            rollingRevdeps[mod] = mod_rolling_revdeps
            rollingOutputRevdeps[mod] = set(mod_rolling_revdeps)
            rollingOutputRevdeps[mod].add(mod)

        # dependency_name -> {CallNode}
        rollingRevdepNames = defaultdict(set)
        # output_name -> {CallNode}
        rollingOutputDepNames = defaultdict(set)

        # build name-based rolling revdeps (starting with the modules)
        for mod in modules:
            for name in mod.outputs:
                rollingRevdepNames[name] = rollingRevdeps[mod]
                rollingOutputDepNames[name] = rollingOutputRevdeps[mod]

        # add on "bare" dependencies (not provided by any modules)
        for name in list(opendeps):
            rollingRevdepNames[name].update(opendeps[name])
            rollingOutputDepNames[name].update(opendeps[name])

            for mod in opendeps[name]:
                rollingRevdepNames[name].update(rollingRevdeps[mod])
                rollingOutputDepNames[name].update(rollingOutputRevdeps[mod])

        self.deps = deps
        self.revdeps = revdeps
        self.rollingOutputRevdeps = rollingOutputDepNames
        self.rollingRevdeps = rollingRevdepNames
        if any(mod.async for mod in modules):
            self.pool = Pool(1)
        else:
            self.pool = None
        self.modules = modules

        if mark_dirty:
            for mod in modules:
                self.state.dirty_outputs.update(mod.outputs)

    def pump(self, input_names=None, output_names=None, async=False):
        """Step through the graph, calling module functions whose input has
        changed or output is required.

        Example::

           for _ in graph.pump():
               pass

        :param input_names: iterable of names for values that have changed; nodes that depend on these quantities will be re-evaluated. If None, default to the set of marked "dirty" inputs
        :param output_names: iterable of names to force computation of; nodes that provide these quantities will be re-evaluated. If None, default to the set of marked "dirty" outputs
        :param async: If True, yield intermediate results whenever an asynchronous module is encountered
        """
        if input_names is None:
            input_names = list(self.state.dirty_inputs)
        if output_names is None:
            output_names = list(self.state.dirty_outputs)

        allCalls = set()
        allCalls.update(self.state.dirty_as_needed)
        for name in input_names:
            allCalls.update(self.rollingRevdeps[name])
        for name in output_names:
            allCalls.update(self.rollingOutputRevdeps[name])
        self.state.dirty_as_needed.update(
            mod for mod in allCalls if mod.as_needed)
        computed = set()

        for mod in self.modules:
            if not mod in allCalls:
                continue

            if mod.as_needed:
                # inputs have changed and someone we will call later
                # in this function needs our value
                if any(
                        any(revdep in allCalls for revdep
                            in self.rollingOutputRevdeps[output]
                            if not revdep.as_needed)
                        for output in mod.outputs):
                    self.state.dirty_as_needed.remove(mod)
                # otherwise, just mark that our inputs have changed
                # and move to the next module
                else:
                    continue

            kwargs = {dep: self.state.scope[mod.remap.get(dep, dep)]
                      for dep in mod.dependencies if mod.remap.get(dep, dep) in self.state.scope}

            try:
                if async and mod.async:
                    thunk = self.pool.apply_async(mod.function, (), kwargs)
                    yield thunk
                    while not thunk.ready():
                        yield thunk
                    outs = thunk.get()
                else:
                    outs = mod.function(**kwargs)
            except Exception as e:
                exception_str = str(e)
                if exception_str not in self.printedExceptions:
                    self.printedExceptions.add(exception_str)
                    raise
                else:
                    continue

            if len(mod.outputs) > 1 and outs:
                for (retname, val) in zip(mod.outputs, outs):
                    self.state.scope[retname] = val
            elif len(mod.outputs):
                self.state.scope[mod.outputs[0]] = outs

            computed.update(mod.outputs)

        for name in input_names:
            self.state.dirty_inputs.discard(name)
        for name in output_names:
            self.state.dirty_outputs.discard(name)

    def pump_tick(self):
        """Perform a single element of work every time it is called. Intended
        for embedding :py:meth:`peact.CallNode.pump` into another
        event loop.
        """
        if self.pumping is None:
            self.pumping = self.pump(async=True)
        else:
            try:
                next(self.pumping)
            except StopIteration:
                self.pumping = None

    def pump_restore(self, names=None, async=False, kwargs={}):
        """Evaluate the graph for a set of given names. Restores the current
        state afterward.

        :param names: List of quantity names to compute
        :param async: If True, compute asynchronously
        :param kwargs: List of quantities to inject into the scope before computing
        """
        old_state = CallGraphState(
            dict(self.state.scope), set(self.state.dirty_inputs),
            set(self.state.dirty_outputs), set(self.state.dirty_as_needed))
        self.inject(**kwargs)
        self.pump(output_names=names, async=async)
        new_state, self.state = self.state, old_state
        return new_state.scope

    def mark_input(self, *args):
        """Marks a quantity for everything that depends on it to be recomputed"""
        self.state.dirty_inputs.update(args)

    def unmark_input(self, *args):
        """Voids a recomputation request for a quantity."""
        for arg in args:
            self.state.dirty_inputs.remove(arg)

    mark = mark_input

    unmark = unmark_input

    def mark_output(self, *args):
        """Marks a quantity for the last node that computes it to be re-run"""
        self.state.dirty_outputs.update(args)

    def unmark_output(self, *args):
        """Voids a recomputation request for a quantity."""
        for arg in args:
            self.state.dirty_outputs.remove(arg)

    def inject(self, *args, **kwargs):
        """Puts a value or set of values into the list of stored quantities
        and marks it as having changed.

        Example::

            graph.inject(temperature=1.5)
            graph.inject({'namespace.value': 13})
        """
        for arg in list(args) + [kwargs]:
            self.state.scope.update(arg)
            self.state.dirty_inputs.update([key for key in arg])
