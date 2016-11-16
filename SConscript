# Copyright (C) 2013 by Carnegie Mellon University.

import os
import re
import fnmatch
import filecmp

#### Decisions are made here.  The rest of the file are functions defintions.
def main():
    build_lazytable('build')

def build_lazytable(variant):
    env = DefaultEnvironment().Clone()
    env['CXX'] = 'g++'
    env.Append(CPPPATH = ['src', 'include'])
    env.Append(LIBS = [
        'pthread', 'zmq', 'boost_system', 'boost_thread', 'tbb', 'numa',
        'tcmalloc_minimal',
        'boost_serialization', 'glog', 'gflags'])
    # The -fPIC flag is necessary to build a shared library
    env.Append(CCFLAGS = '-Wall -Wno-sign-compare -g -fPIC')

    if (env['build_debug'] == '1'):
        env.Append(CCFLAGS = '-ggdb')
    else:
        env.Append(CCFLAGS = '-O3')

    # build client library
    src_files = ['src/client/iterstore.cpp']
    src_files.append('src/client/clientlib.cpp')
    src_files.append('src/client/clientlib-static-cache.cpp')
    src_files.append('src/client/clientlib-thread-cache.cpp')
    src_files.append('src/client/clientlib-viter.cpp')
    src_files.append('src/client/clientlib-pusher-thread.cpp')
    src_files.append('src/client/clientlib-puller-thread.cpp')
    src_files.append('src/client/encoder-decoder.cpp')
    src_files.append('src/common/background-worker.cpp')
    src_files.append('src/common/work-puller.cpp')
    src_files.append('src/common/work-pusher.cpp')
    src_files.append('src/common/router-handler.cpp')
    src_files.append('src/server/server-encoder-decoder.cpp')
    src_files.append('src/server/tablet-server.cpp')
    src_files.append('src/server/metadata-server.cpp')
    src_files.append('src/server/server-entry.cpp')
    clientlib = env.Library('iterstore', src_files)
    env.Install('lib', clientlib)

    # how to build applications
    app_env = env.Clone()
    app_env.Append(CPPPATH = ['apps/shared'])
    app_env.Append(LIBPATH = ['lib'])
    app_env.Append(LIBPATH = ['build'])
    app_env.Prepend(LIBS = ['iterstore', 'boost_program_options'])

    # build mf
    app_env.Program(['apps/mf/mf.cpp'])

    # build lda
    app_env.Program(['apps/lda/lda.cpp'])

    # build mlr
    app_env.Program(['apps/mlr/mlr.cpp'])

    # build pagerank
    app_env.Program(['apps/pagerank/pagerank.cpp'])

# end of build_all()

#### Lint related stuff

def find_files():
    patterns = ['*.cpp', '*.c', '*.hpp', '*.h']
    for root, dirs, files in os.walk('..'):
        if root.startswith('../.git') or root.startswith('../build') or \
           root.startswith('../lint') :
            continue
        for basename in files:
            if basename.endswith('boost_serialization_unordered_map.hpp'):
                continue
            if any(fnmatch.fnmatch(basename, p) for p in patterns) :
                filename = os.path.join(root, basename)
                yield filename

# The body of self-defined builder .Lint
def lint_action(target, source, env):
    # target and source are lists of file objects
    src = str(source[0])      # in source tree
    to_lint = 'lint/' + src  # in lint tree

    # Make xxx.hpp to xxx.hpp.cpp to fool cpplint.py into linting .hpp files
    # Copy the file to lint tree if necessary
    if fnmatch.fnmatch(src, '*.hpp'):
        to_lint += '.cpp'
    if fnmatch.fnmatch(src, '*.h'):
        to_lint += '.c'
    if (not (os.path.isfile(to_lint) and filecmp.cmp(src, to_lint))) :
        os.system('cp %s %s' % (src, to_lint))

    lint_options = '--filter=-readability/streams' + \
               ',-runtime/references' + \
               ',-whitespace/newline'
    lintcmd = 'python scripts/cpplint.py %s %s' % (lint_options, to_lint)
    print lintcmd
    status = os.system(lintcmd)
    if (status == 0):
        datecmd = 'date > ' + str(target[0])
        print datecmd
        os.system(datecmd)
    return status

main()

# to google-style a cpp/hpp file: "astyle -a --indent=spaces=2 <file>"
