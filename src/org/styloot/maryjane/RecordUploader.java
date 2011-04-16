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
    private Thread uploadThread;

    public RecordUploader(File myStagingArea) throws IOException {
	stagingArea = myStagingArea;
	uploadThread = new Thread(new SubmissionThread());
	uploadThread.start();
    }

    public void addRemoteLocation(String nm, String rp) {
	locations.put(nm, new RemoteLocation(nm, new Path(rp)));
    }

    public void queueFileForUpload(String nm, File inFile, String remoteName) throws InterruptedException {
	RemoteLocation loc = locations.get(nm);
	File stagedFile = new File(loc.stagingArea(), remoteName);
	if (!inFile.renameTo(stagedFile)) {
	    log.error("Unable to copy file " + inFile + " to staging area " + stagedFile + ". Data may NOT be committed to the database.");
	    return;
	}
	log.debug("Successfully copied file " + inFile + " to " + stagedFile);
	UploadRequest req = new UploadRequest(loc, stagedFile, remoteName);
	if (!queue.offer(req, QUEUE_OFFER_DELAY, TimeUnit.MILLISECONDS)) {
	    log.error("Queue is saturated! Unable to commit data in " + inFile + ". Left in staging area. Data may NOT be committed to the database.");
	    return;
	}
	log.info("Accepted " + req);
    }

    public void finish() throws InterruptedException {
	uploadThread.interrupt();
    }

    private class SubmissionThread implements Runnable {
	public void run() {
	    log.info("Starting submitter thread.");
	    UploadRequest req;
	    while (true) {
		try {
		    req = queue.take();
		} catch (InterruptedException e) {
		    log.error("Submission thread interrupted.");
		    return;
		}
		if (req != null)
		    transmitFile(req);
		else
		    break;
	    }
	}

	private void transmitFile(UploadRequest req) {
	    log.info("Attempting to upload file " + req.remoteName + " to location " + req.loc.remotePath);
	    try {
		uploadFile(req);
		log.info("Successfully uploaded file " + req.remoteName + " to location " + req.loc.remotePath);
	    } catch (IOException e) {
		log.error("Received IOException while attempting to upload file " + req.remoteName + " to " + req.loc + ". Will retry.");
	    }
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

	public String toString() {
	    return "UploadRequest(loc=" + loc + ", file=" + file + ", remoteName=" + remoteName + ")";
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