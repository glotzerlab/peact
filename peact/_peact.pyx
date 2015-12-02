# distutils: language = c++
# cython: embedsignature=True

import inspect
from multiprocessing import Pool
from collections import defaultdict

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

class CallGraph:
    def __init__(self):
        self.modules = []
        self.moduleLists = []
        # map module -> modules it depends on
        self.deps = defaultdict(list)
        # map module -> modules that depend on it
        self.revdeps = defaultdict(list)
        # map property name -> modules that it depends on, even transiently
        self.rollingDeps = defaultdict(set)
        # map property name -> modules that depend on it, even transiently
        self.rollingRevdeps = defaultdict(set)
        self.pool = None
        self.scope = {}
        self.dirty = set()
        self.pumping = None
        self.printedExceptions = set()

    def register(self, function, *args, **kwargs):
        node = CallNode(function, *args, **kwargs)
        self.moduleLists.append([node])
        return function

    def register_deferred(self, target):
        self.moduleLists.append(target)
        return target

    def unregister(self, function, rebuild=True):
        index = [i for (i, mod) in enumerate(self.moduleLists)
                 if len(mod) == 1 and mod[0].function == function][0]
        del self.modules[index]
        if rebuild:
            self.rebuild()

    def unregister_deferred(self, target, rebuild=True):
        index = [i for (i, mod) in enumerate(self.moduleLists)
                 if mod is target][0]
        del self.modules[index]
        if rebuild:
            self.rebuild()

    def clear(self):
        self.modules = []
        self.rebuild()

    def rebuild(self):
        outputs = set()
        modules = []

        for mods in self.moduleLists:
            modules.extend(mods)

        # build the dependency graph
        deps = defaultdict(list)
        revdeps = defaultdict(list)
        # opendeps[name] is a list of modules which depend on name
        # when nothing which provides name has been added to the graph
        # previously; these should be notified when name is injected
        opendeps = defaultdict(list)
        providers = {}

        for mod in modules:
            for dep in [mod.remap.get(d, d) for d in mod.dependencies]:
                if dep in providers:
                    deps[mod].append(providers[dep])
                    revdeps[providers[dep]].append(mod)
                else:
                    opendeps[dep].append(mod)
            for out in mod.outputs:
                providers[out] = mod

        # rollingDeps and rollingRevdeps are indexed by module id
        rollingDeps = defaultdict(set)
        rollingRevdeps = defaultdict(set)
        # rollingDepNames and rollingRevdepNames are indexed by output
        # name, not module id
        rollingDepNames = defaultdict(set)
        rollingRevdepNames = defaultdict(set)

        for dep in list(deps):
            rolling = []
            toGrab = list(deps[dep])

            while toGrab:
                val = toGrab.pop()
                rolling.append(val)
                toGrab.extend(deps[val])

            rollingDeps[dep] = set(rolling)

        for dep in list(revdeps):
            rolling = []
            toGrab = list(revdeps[dep])

            while toGrab:
                val = toGrab.pop()
                rolling.append(val)
                toGrab.extend(revdeps[val])

            rollingRevdeps[dep] = set(rolling)

        for mod in modules:
            for name in mod.outputs:
                rollingDepNames[name] = rollingDeps[mod]
                if not mod.as_needed:
                    rollingRevdepNames[name] = rollingRevdeps[mod]

        for name in list(opendeps):
            rollingRevdepNames[name].update(opendeps[name])
            for mod in opendeps[name]:
                rollingRevdepNames[name].update(rollingRevdeps[mod])

        self.deps = deps
        self.revdeps = revdeps
        self.rollingDeps = rollingDepNames
        self.rollingRevdeps = rollingRevdepNames
        if any(mod.async for mod in modules):
            self.pool = Pool(1)
        else:
            self.pool = None
        self.modules = modules
        for mod in modules:
            self.dirty.update([mod.remap.get(d, d) for d in mod.dependencies])
            self.dirty.update(mod.outputs)

    def pump(self, names=None, async=False):
        if names is None:
            names = list(self.dirty)

        allCalls = set()
        for name in names:
            allCalls.update(self.rollingDeps[name])
            allCalls.update(self.rollingRevdeps[name])
        computed = set()

        for mod in [mod for mod in self.modules if mod in allCalls]:
            kwargs = {dep: self.scope[mod.remap.get(dep, dep)]
                      for dep in mod.dependencies if mod.remap.get(dep, dep) in self.scope}

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
                if str(e) not in self.printedExceptions:
                    self.printedExceptions.add(str(e))
                    raise
                else:
                    continue

            if len(mod.outputs) > 1:
                for (retname, val) in zip(mod.outputs, outs):
                    self.scope[retname] = val
            elif len(mod.outputs):
                self.scope[mod.outputs[0]] = outs

            computed.update(mod.outputs)

        for name in names:
            self.dirty.discard(name)

    def pump_tick(self):
        if self.pumping is None:
            self.pumping = self.pump(async=True)
        else:
            try:
                next(self.pumping)
            except StopIteration:
                self.pumping = None

    def pump_restore(self, names=None, async=False, kwargs={}):
        scope = dict(self.scope)
        self.inject(**kwargs)
        self.pump(names, async)
        result, self.scope = self.scope, scope
        return result

    def mark(self, *args):
        """Marks a property for recomputation"""
        self.dirty.update(args)

    def unmark(self, *args):
        for arg in args:
            self.dirty.remove(arg)

    def inject(self, *args, **kwargs):
        for arg in list(args) + [kwargs]:
            self.scope.update(arg)
            self.dirty.update([key for key in arg])
