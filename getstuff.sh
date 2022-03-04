#!/bin/bash
if [ ! -d velocypack ] ; then
    git clone https://github.com/arangodb/velocypack
fi
if [ ! -d rocksdb ] ; then
    git clone https://github.com/facebook/rocksdb
fi
if test ! -d docopt.cpp ; then
    git clone https://github.com/docopt/docopt.cpp
fi
if test ! -d liburing ; then
    git clone https://github.com/axboe/liburing
    cd liburing
    ./configure
    make -j 16
    cd ..
fi
