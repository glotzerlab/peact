.. peact documentation master file, created by
   sphinx-quickstart on Thu Dec  3 13:04:13 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to peact's documentation!
=================================

**Peact** is a library for reactive programming in python.

Introduction
------------

As an analogy for peact, consider the process of building
software. The predominant build method on UNIX systems involves
*Makefiles*, which specify files which can be created and "recipes" to
create each file. Each file has a number of dependencies, which the
make system will ensure have been created before the recipe for the
file is run.

Peact is a library which enables a similar method of programming
inside python instead of on the filesystem. Rather than the `make`
program, peact is the orchestrator of activity. Instead of files,
peact deals with "quantities," each with a particular name. The
recipes and file contents of `make` are replaced with python functions
and python objects, respectively.

In other words, peact allows you to string together python functions
which consume and produce quantities. As input values change, nodes in
the graph are updated in response to these changes, potentially
updating other nodes as well.

Reactive Python API
-------------------

To use peact, create a :py:class:`peact.CallGraph` object and
:py:func:`peact.CallGraph.register` :py:class:`peact.CallNode` objects
(representing functions) on it. Input values can come from nodes which
themselves have no inputs or by calling
:py:func:`peact.CallGraph.inject` to immediately set values.

After the :py:class:`peact.CallGraph` has been prepared,
:py:func:`peact.CallGraph.pump` can be used to step through the graph
and call each registered function which needs to be updated. Values
are stored in the `scope` member of a :py:class:`peact.CallGraph`.

.. autoclass:: peact.CallNode
   :members:

.. autoclass:: peact.CallGraph
   :members:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
