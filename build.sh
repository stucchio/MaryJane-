#!/bin/sh

rm -rf gen-java
echo "Generating thrift interface for java."
thrift --gen java maryjane.thrift
echo "Patching file to remove logger."
patch -p0 < patches/remove_logger.patch
echo "Now building."
ant compile
ant maryjane