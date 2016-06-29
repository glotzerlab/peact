
import traceback
import importlib
import json
import os
import tempfile

from .. import CallGraph

_modulesLoaded = 0

def loadModuleFromDesc(desc):
    """Attempt to load a peact :py:class:`Module` from a dictionary
    description. Returns None if there is an error."""
    global _modulesLoaded

    try:
        modFile = None
        if desc['type'] in ['source', 'file']:
            if desc['type'] == 'source':
                modSource = desc['source']

                with tempfile.NamedTemporaryFile(suffix='.py', delete=False, mode='w') as temp:
                    temp.file.write(modSource)
                    modFile = temp.name
            else:
                modFile = desc['file']
                modSource = open(modFile, 'r').read()

            newMod = importlib.machinery.SourceFileLoader(
                'peact_modules{}'.format(_modulesLoaded), modFile).load_module()
            _modulesLoaded += 1
            # newMod = imp.new_module('testModule')
            # exec(modSource, newMod.__dict__)
            if hasattr(newMod, 'Module'):
                result = newMod.Module
            else:
                result = None
        else:
            result = None
    finally:
        if desc['type'] == 'source' and modFile and os.path.exists(modFile):
            os.remove(modFile)
    return result

class ModuleList:
    """Holds a list of modules and their metadata. Wraps basic operations
    performed on them.

    """

    def __init__(self):
        self.modules = []
        self.metadata = []
        self.graph = CallGraph()

    def add(self, desc):
        cls = loadModuleFromDesc(desc)
        if cls is None:
            raise RuntimeError('Failed to understand module description')
        module = cls(self.graph)
        if 'state' in desc:
            module.deserialize(desc['state'])

        self.modules.append(module)
        self.metadata.append(desc)
        self.graph.rebuild()

        return module

    def remove(self, index=-1):
        module = None
        try:
            module = self.modules.pop(index)
            module.cleanup()
        except:
            if module is not None:
                if index < 0:
                    self.modules.insert(len(self.modules) + 1 + index, module)
                else:
                    self.modules.insert(index, module)
            raise
        metadata = self.metadata.pop(index)
        self.graph.rebuild()

        return (module, metadata)

    def move(self, start, target):
        minidx = min(start, target)
        metadata = list(self.metadata)
        while len(self.modules) > minidx:
            try:
                self.remove()
            except Exception:
                print(traceback.format_exc(3))

        metadata.insert(target, metadata.pop(start))

        for desc in metadata[minidx:]:
            self.add(desc)

    def save(self, fname):
        for (module, metadata) in zip(self.modules, self.metadata):
            try:
                module.serialize(metadata.setdefault('state', {}))
            except Exception as e:
                print(traceback.format_exc(3))
        json.dump(self.metadata, open(fname, 'w'))

class Module:
    """Base class for all peact modules. Performs simple initialization
    and serialization of some data."""

    name = 'default module name'

    autoserialize = []

    def __init__(self, graph):
        self.graph = graph
        self.registeredCalls = []
        self.deferredCalls = []

    def _autoserialize(self, src, dest):
        for name in self.autoserialize:
            try:
                dest[name] = src[name]
            except KeyError:
                pass

    def cleanup(self):
        while self.registeredCalls:
            self.graph.unregister(self.registeredCalls.pop())

        while self.deferredCalls:
            self.deferredCalls.pop()

    def serialize(self, target):
        self._autoserialize(self.graph.scope, target)

    def deserialize(self, target):
        self._autoserialize(target, self.graph.scope)

    def register(self, *args, **kwargs):
        self.registeredCalls.append(self.graph.register(*args, **kwargs))
