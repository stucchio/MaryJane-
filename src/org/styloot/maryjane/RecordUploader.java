package org.styloot.maryjane;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import java.util.*;
import java.text.*;
import java.util.concurrent.*;
import java.io.*;
import org.apache.log4j.*;

public class RecordUploader {
    private static final Logger log = Logger.getLogger(RecordUploader.class);

    private final File stagingArea;

    private BlockingQueue<UploadRequest> queue = new ArrayBlockingQueue<UploadRequest>(1024, false);

    public static long QUEUE_OFFER_DELAY = 500;

    public RecordUploader(File myStagingArea) throws IOException {
	stagingArea = myStagingArea;
    }

    public void addRemoteLocation(String nm, String rp) {
	locations.put(nm, new RemoteLocation(nm, new Path(rp)));
    }

    public void queueFileForUpload(String nm, File inFile, String remoteName) throws InterruptedException {
	RemoteLocation loc = locations.get(nm);
	File stagedFile = new File(loc.stagingArea(), remoteName);
	if (!inFile.renameTo(stagedFile)) {
	    log.error("Unable to copy file " + inFile + " to staging area " + loc.stagingArea() + ". Data may NOT be committed to the database.");
	}
	if (!queue.offer(new UploadRequest(loc, stagedFile, remoteName), QUEUE_OFFER_DELAY, TimeUnit.MILLISECONDS)) {
	    log.error("Queue is saturated! Unable to commit data in " + inFile + ". Left in staging area. Data may NOT be committed to the database.");
	}
    }

    private void uploadFile(UploadRequest req) throws IOException {
	Path pathToFlush = new Path(req.loc.remotePath, req.remoteName);
	FileSystem remoteFileSystem = pathToFlush.getFileSystem(new Configuration());
	remoteFileSystem.moveFromLocalFile(new Path(req.file.getAbsolutePath()), pathToFlush);
    }

    private Map<String,RemoteLocation> locations = new HashMap<String,RemoteLocation>();

    private class UploadRequest {
	public RemoteLocation loc;
	public File file;
	public String remoteName;

	public UploadRequest(RemoteLocation l, File f, String rn) {
	    loc = l;
	    file = f;
	    remoteName = rn;
	}
    }

    private class RemoteLocation {
	public String name;
	public Path remotePath;

	public RemoteLocation(String nm, Path rp) {
	    name = nm;
	    remotePath = rp;
	}

	public File stagingArea() {
	    File result = new File(stagingArea, name);
	    result.mkdirs();
	    return result;
	}
    }
}