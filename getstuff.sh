#!/bin/bash
if [ ! -d velocypack ] ; then
    git clone https://github.com/arangodb/velocypack
fi
if [ ! -d rocksdb ] ; then
    git clone https://github.com/facebook/rocksdb
fi
