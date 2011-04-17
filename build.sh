#!/bin/sh

echo "Generating thrift interface for java."
thrift --gen java maryjane.thrift
echo "Now building."
ant compile
ant maryjane
rm -rf gen-java
mv maryjane.jar build/
echo "Java library built at build/maryjane.jar"
echo "Building python interface"
thrift --gen py maryjane.thrift
tar czf python-maryjane.tgz -C gen-py maryjane
rm -rf gen-py/
mv python-maryjane.tgz build/
echo "Python library built at build/python-maryjane.tgz"