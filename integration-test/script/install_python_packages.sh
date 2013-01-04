#!/bin/bash
# install the missing python packages
#set -x
script_dir=$(dirname $0)
pushd $script_dir
file=../lib/simplejson-2.1.2.tar.gz
gunzip -c $file | tar xf -
cd simplejson-2.1.2
python setup.py build
python setup.py install -f --root=/tmp
cd ..
cp -rf `find /tmp/ -name "simplejson" -type d | head -n 1` .
rm -rf simplejson-2.1.2
popd
