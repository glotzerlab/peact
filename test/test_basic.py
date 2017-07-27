import unittest
import peact

class BasicTest(unittest.TestCase):
    def test_basic_flow(self):
        f = lambda x: x + 3
        g = lambda f: f*7

        graph = peact.CallGraph()
        graph.register(f, ['f'])
        graph.register(g, ['g'])
        graph.inject(x=3)

        graph.rebuild()
        for _ in graph.pump():
            pass

        self.assertEqual(graph.scope['g'], 42)

        graph.inject(x=4)
        for _ in graph.pump():
            pass

        self.assertEqual(graph.scope['g'], 49)

        graph.inject(f=4)
        for _ in graph.pump():
            pass

        self.assertEqual(graph.scope['g'], 28)

    def test_broken_graph(self):
        graph = peact.CallGraph()
        graph.register(lambda x: 1 + x, ['f'])
        graph.register(lambda h: 2*h, ['broken_dependency'])
        graph.rebuild()
        graph.inject(x=3)

        with self.assertRaises(TypeError):
            for _ in graph.pump():
                pass

        # missing value filled, can proceed without error now
        graph.inject(h=4)

        for _ in graph.pump():
            pass

    def test_rolling_deps(self):
        #  f   g
        #  | / |
        #  a   b
        #  |   |
        #  x   y
        graph = peact.CallGraph()
        graph.register(lambda x: 1 + x, ['a'])
        graph.register(lambda y: 2 + y, ['b'])
        graph.register(lambda a, b: a*b, ['f'])
        graph.register(lambda a: 17*a, ['g'])
        graph.rebuild()

        graph.inject(x=1, y=3)
        for _ in graph.pump():
            pass

        self.assertEqual(len(graph.rollingRevdeps['x']), 3)
        self.assertEqual(len(graph.rollingRevdeps['y']), 2)
        self.assertEqual(len(graph.rollingRevdeps['a']), 2)
        self.assertEqual(len(graph.rollingRevdeps['b']), 1)
        self.assertEqual(len(graph.rollingRevdeps['g']), 0)
        self.assertEqual(len(graph.rollingRevdeps['f']), 0)

        # note that depsByName only provides _node objects_, so
        # "bare" inputs to the graph (that must be injected; i.e., x
        # and y here) don't count toward these counts
        for key in 'abfgxy':
            # everything but x and y simply includes its own node in
            # the count, so it is count + 1 for rollingOutputDeps
            self.assertEqual(len(graph.rollingRevdeps[key]) + (key not in 'xy'),
                             len(graph.rollingOutputDeps[key]))

    def test_not_marked(self):
        graph = peact.CallGraph()

        def throws(bad):
            assert False

        graph.register(throws)
        graph.register((lambda x: True), ['a'])

        graph.rebuild(False)
        graph.mark_output('a')

        graph.inject(x=3)
        for _ in graph.pump():
            pass

    def test_as_needed(self):
        graph = peact.CallGraph()

        def throws(bad):
            assert False

        graph.register(throws, as_needed=True)
        graph.register((lambda x: True), ['a'])
        graph.register((lambda throws: True), ['takes_throws'])

        graph.mark_output('a')
        graph.rebuild()
        graph.inject(bad=10)

        with self.assertRaises(AssertionError):
            for _ in graph.pump():
                pass

    def test_overwritten(self):
        graph = peact.CallGraph()

        def throws(bad_arg):
            assert False

        graph.register(throws, ['a'])
        graph.register((lambda x: True), ['a'])

        graph.mark_output('a')
        graph.rebuild(False)
        graph.inject(x=3)

        for _ in graph.pump():
            pass

if __name__ == '__main__': unittest.main()
