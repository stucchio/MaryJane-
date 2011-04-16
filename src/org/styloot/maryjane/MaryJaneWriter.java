package org.styloot.maryjane;

import org.styloot.maryjane.jsonsimple.*;

import org.apache.hadoop.fs.*;
import org.apache.log4j.*;

import java.util.*;
import java.io.*;


public class MaryJaneWriter {
    private static final Logger log = Logger.getLogger(MaryJaneWriter.class);

    File localDir;
    Map<String,StreamHandler> streams = new HashMap<String,StreamHandler>();
    FileUploader fileUploader;

    public MaryJaneWriter(File myLocalDir) throws IOException {
        localDir = myLocalDir;
        if (!localDir.exists()) {
            throw new IOException("Local directory " + myLocalDir + " does not exist.");
        }
        fileUploader = new FileUploader();
    }

    public long addRecord(String streamname, String key, String value) throws IOException {
        StreamHandler sh = getStreamHandler(streamname);
        return sh.addRecord(key, value);
    }

    public long sync(String streamname) throws IOException, InterruptedException {
        StreamHandler sh = getStreamHandler(streamname);
        return sh.submit();
    }

    public void syncAll() throws IOException, InterruptedException {
	for (StreamHandler sh : streams.values()) {
	    sh.submit();
	}
    }

    public void addStreamHandler(String name, String prefix, boolean compress, boolean noBuffer, RemoteLocation remoteLocation) throws IOException {
	if (streams.containsKey(name))
	    throw new IndexOutOfBoundsException("MaryJaneWriter already has a StreamHandler with name " + name);
        StreamHandler sh = new StreamHandler(name, fileUploader, prefix, compress, localDir, noBuffer, remoteLocation);
        streams.put(name, sh);
    }

    private StreamHandler getStreamHandler(String name) {
        StreamHandler sh = streams.get(name);
        if (sh == null)
            throw new IndexOutOfBoundsException("Could not find streamhandler for name " + name);
        return sh;
    }

    public void setRecordsBeforeSubmit(String streamname, long records) {
        getStreamHandler(streamname).setRecordsBeforeSubmit(records);
    }

    //Set submit interval, in seconds.
    public synchronized void setSubmitInterval(String streamname, long submitInterval) {
	getStreamHandler(streamname).setSubmitInterval(submitInterval);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        MaryJaneWriter mj = new MaryJaneWriter(new File("/tmp/maryjane"));

        mj.addStreamHandler("foo", "foorecord", false, false,
			    new RemoteLocation("foo", args[0]));
        mj.addStreamHandler("bar", "barrecord", false, false,
			    new RemoteLocation("bar", args[1]));

	mj.setSubmitInterval("bar", 60);
	//mj.setRecordsBeforeSubmit("foo", 100);

        for (int i=0;i<10;i++) {
            for (int j=0;j<500;j++) {
                mj.addRecord("foo", i + "," + j, UUID.randomUUID().toString());
                mj.addRecord("bar", i + "," + j, UUID.randomUUID().toString());
            }
        }
        System.out.println("Finished submitting...");
        Thread.sleep(1000*30);
	System.out.println("Waited 30 seconds");
        Thread.sleep(1000*29);
	System.out.println("Waited 59 seconds");
    }

}


