package org.styloot.maryjane.thriftserver;

import java.io.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.protocol.TBinaryProtocol.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.log4j.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import org.styloot.maryjane.*;
import org.styloot.maryjane.jsonsimple.*;
import org.styloot.maryjane.gen.*;

public class Server {
    private static final Logger log = Logger.getLogger(Server.class);

    private int port;
    private MaryJaneWriter writer;

    public Server(int myPort, File localPath, JSONObject json) throws IOException {
	port = myPort;
	log.info("Starting MaryJane server on port " + myPort);
	System.out.println(json);
	String filesystem = (String)json.get("filesystem");
	Path basePath = new Path(filesystem);
	log.info("Connecting to remote filesystem " + basePath);

	writer = new MaryJaneWriter(localPath);

	JSONObject streams = (JSONObject)json.get("streams");
	for (Object nameObj : streams.keySet()) {
	    String name = (String)nameObj;
	    JSONObject stream = (JSONObject)streams.get(name);
	    String remotePath = (String)stream.get("path");
	    assertNotNull(remotePath, "Parameter 'path' in stream " + name + " must not be null.");
	    String prefix = (String)stream.get("prefix");
	    assertNotNull(prefix, "Parameter 'prefix' in stream " + name + " must not be null.");

	    writer.addStreamHandler(name, prefix, false, false,
				    new RemoteLocation(name, new Path(basePath, remotePath)));

	}
    }

    public Server(int myPort, MaryJaneWriter myWriter) {
	port = myPort;
	writer = myWriter;
    }

    private void assertNotNull(String arg, String message) {
	if (arg == null)
	    throw new IllegalArgumentException(message);
    }

    private void start() {

        try {
            TServerSocket serverTransport = new TServerSocket(10289);
            MaryJane.Processor processor = new MaryJane.Processor(new MaryJaneServerImpl(writer));
            Factory protFactory = new TBinaryProtocol.Factory(true, true);
            TServer server = new TThreadPoolServer(processor, serverTransport, protFactory);
            System.out.println("Starting server on port 10289 ...");
            server.serve();
        }
        catch(TTransportException e) {
            e.printStackTrace();
        }
    }

    private static String readFile(Path configPath) throws IOException {
	FileSystem configFileSystem = configPath.getFileSystem(new Configuration());
	FSDataInputStream in = configFileSystem.open(configPath);

	byte[] buffer = new byte[4086];
	StringBuilder builder = new StringBuilder();

	int bytesRead = 1;
	while (bytesRead > 0) {
	    bytesRead = in.read(buffer);
	    if (bytesRead > 0) {
		byte[] toCopy = new byte[bytesRead];
		System.arraycopy(buffer, 0,toCopy,0,bytesRead);
		builder.append(new String(toCopy));
		}
	}
	return builder.toString();
    }

    public static void main(String[] args) throws IOException {
 	int port = new Integer(args[0]);
	File localPath = new File(args[1]);

	Path configPath = new Path(args[2]);

	String configString = readFile(configPath);
	System.out.println(configString);
	JSONObject config = (JSONObject)JSONValue.parse(configString);


        Server srv = new Server(port, localPath, config);
        srv.start();
    }
}