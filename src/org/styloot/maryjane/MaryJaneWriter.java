package org.styloot.maryjane;

import org.styloot.maryjane.jsonsimple.*;

import org.apache.hadoop.fs.*;
import org.apache.log4j.*;

import org.styloot.maryjane.gen.*;

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

    public long addRecord(String streamname, String key, String value) throws IOException, MaryJaneStreamNotFoundException, MaryJaneFormatException {
        StreamHandler sh = getStreamHandler(streamname);
        return sh.addRecord(key, value);
    }

    public long sync(String streamname) throws IOException, InterruptedException, MaryJaneStreamNotFoundException{
        StreamHandler sh = getStreamHandler(streamname);
        return sh.submit();
    }

    public void syncAll() throws IOException, InterruptedException {
	for (StreamHandler sh : streams.values()) {
	    sh.submit();
	}
    }

    public void addStreamHandler(String name, String prefix, boolean compress, boolean noBuffer, RemoteLocation remoteLocation) throws IOException, InterruptedException {
	if (streams.containsKey(name))
	    throw new IndexOutOfBoundsException("MaryJaneWriter already has a StreamHandler with name " + name);
        StreamHandler sh = new StreamHandler(name, fileUploader, prefix, compress, localDir, noBuffer, remoteLocation);
        streams.put(name, sh);
    }

    private StreamHandler getStreamHandler(String name) throws MaryJaneStreamNotFoundException {
        StreamHandler sh = streams.get(name);
        if (sh == null)
            throw new MaryJaneStreamNotFoundException("Could not find streamhandler for name " + name);
        return sh;
    }

    public void setRecordsBeforeSubmit(String streamname, long records) throws MaryJaneStreamNotFoundException {
        getStreamHandler(streamname).setRecordsBeforeSubmit(records);
    }

    public void setFileSizeLimit(String streamname, long sizeLimit) throws MaryJaneStreamNotFoundException {
        getStreamHandler(streamname).setFileSizeLimit(sizeLimit);
    }

    //Set submit interval, in seconds.
    public void setSubmitInterval(String streamname, long submitInterval) throws MaryJaneStreamNotFoundException {
	getStreamHandler(streamname).setSubmitInterval(submitInterval);
    }

    public void setFlushInterval(String streamname, long interval) throws MaryJaneStreamNotFoundException {
	getStreamHandler(streamname).setFlushInterval(interval);
    }

    public static void main(String[] args) throws IOException, InterruptedException, MaryJaneStreamNotFoundException, MaryJaneFormatException {
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


