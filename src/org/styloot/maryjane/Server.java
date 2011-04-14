package org.styloot.maryjane;

import java.io.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.protocol.TBinaryProtocol.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;

import org.styloot.maryjane.*;
import org.styloot.maryjane.gen.*;

public class Server
{
  private void start()
  {
    try
    {
      TServerSocket serverTransport = new TServerSocket(7911);
      MaryJane.Processor processor = new MaryJane.Processor(new MaryJaneServerImpl());
      Factory protFactory = new TBinaryProtocol.Factory(true, true);
      TServer server = new TThreadPoolServer(processor, serverTransport, protFactory);
      System.out.println("Starting server on port 7911 ...");
      server.serve();
    }catch(TTransportException e)
    {
      e.printStackTrace();
    }
  }

  public static void main(String[] args)
  {
    Server srv = new Server();
    srv.start();
  }
}