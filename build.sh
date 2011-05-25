#!/bin/bash

function remove_slf4j(){
    local filepath=$1;
    local tmppath=`mktemp`;
    sed '/slf4j/d' $filepath | sed '/LOGGER/d' > $tmppath
    mv $tmppath $1
}

echo "Generating thrift interface for java."
thrift --gen java maryjane.thrift
echo "Stripping slf4j from generated thrift files."
for fn in `ls gen-java/org/styloot/maryjane/gen/*.java`;
do
    remove_slf4j $fn;
done;

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