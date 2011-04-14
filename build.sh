#!/bin/sh

thrift --gen java maryjane.thrift
patch -p0 < patches/remove_logger.patch
ant compile