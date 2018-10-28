#!/usr/bin/env python

import sys
from distutils.command.build_ext import build_ext
from distutils.core import Extension, setup

with open('peact/version.py') as version_file:
    exec(version_file.read())

if '--cython' in sys.argv:
    from Cython.Build import cythonize
    sys.argv.remove('--cython')
    modules = cythonize('peact/_peact.pyx')
else:
    sources = ['peact/_peact.cpp']
    modules = [Extension('peact._peact', sources=sources)]

setup(name='peact',
      author='Matthew Spellings',
      author_email='mspells@umich.edu',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: BSD License',
      ],
      description='Python reactive library',
      ext_modules=modules,
      license='BSD',
      packages=['peact', 'peact.modules', 'peact.export'],
      project_urls={
          'Documentation': 'http://peact.readthedocs.io/',
          'Source': 'https://bitbucket.org/glotzer/peact'
          },
      version=__version__,
)
