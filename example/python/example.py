#!/bin/python

import sys

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from maryjane.MaryJane import Client

transport = TSocket.TSocket('localhost', 10289)
transport = TTransport.TBufferedTransport(transport)
protocol = TBinaryProtocol.TBinaryProtocol(transport)

client = Client(protocol)
transport.open()

for i in range(256):
    client.addRecord("mary", str(i), str(i*i))
    client.addRecord("jane", str(i), str(i*i))

#client.sync("mary")

