
import networkx

def export_networkx(callGraph):
    nxGraph = networkx.DiGraph()
    node_labels = {}

    modules = sum(callGraph.moduleLists, [])
    for node in modules:
        nxGraph.add_node(node)
        try:
            node_labels[node] = node.function.__name__
        except AttributeError: # functools.partial instances
            node_labels[node] = node.function.func.__name__

    for src in callGraph.revdeps:
        for target in callGraph.revdeps[src]:
            nxGraph.add_edge(src, target)

    return nxGraph, node_labels
