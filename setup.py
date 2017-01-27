#!/usr/bin/env python

import sys
from distutils.command.build_ext import build_ext
from distutils.core import Extension, setup

if '--cython' in sys.argv:
    from Cython.Build import cythonize
    sys.argv.remove('--cython')
    modules = cythonize('peact/_peact.pyx')
else:
    sources = ['peact/_peact.cpp']
    modules = [Extension('peact._peact', sources=sources)]

setup(name='peact',
      version='0.1',
      description='Python reactive library',
      author='Matthew Spellings',
      author_email='mspells@umich.edu',
      url='',
      packages=['peact', 'peact.modules', 'peact.export'],
      ext_modules=modules
)
