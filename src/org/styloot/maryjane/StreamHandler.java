package org.styloot.maryjane;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import java.util.*;
import java.text.*;
import java.util.concurrent.*;
import java.io.*;
import org.apache.log4j.*;

public class StreamHandler {
    private static final Logger log = Logger.getLogger(StreamHandler.class);

    private FileUploader uploader;
    private String namePrefix;
    private boolean useCompression;
    private String name;
    private final File localPath;
    private final File localDataPath;
    private final File localStagingPath;
    private boolean noMemoryBuffer = false;
    private File bufferFile;
    private PrintStream outStream;
    private RemoteLocation remoteLocation;

    public long recordLimitBeforeSubmit = -1;
    private long lastSubmitTime = 0;
    private long submitInterval = -1;
    private long recordsBeforeSubmit = -1;

    private static long OFFER_TIMEOUT = 500;

    public StreamHandler(String myName, FileUploader myUploader, String myPrefix, boolean compress, File localDir, boolean noBuffer, RemoteLocation myRemoteLocation) throws IOException {
	name = myName;
	uploader = myUploader;
        namePrefix = myPrefix;
        useCompression = compress;
	noMemoryBuffer = noBuffer;
	remoteLocation = myRemoteLocation;

	if (!localDir.exists())
	    throw new IOException("Local directory " + localDir + " does not exist!");
        localPath = new File(localDir, namePrefix);
        localPath.mkdirs();
	localDataPath = new File(localPath, "data");
	localDataPath.mkdirs();
	localStagingPath = new File(localPath, "staging");
	localStagingPath.mkdirs();

	newBufferFile();
    }

    public String toString() {
	return "StreamHandler(" + name + ", loc=" + remoteLocation + ")";
    }

    long recordsWritten = 0;
    public synchronized long addRecord(String key, String value) throws StreamHandlerException, IOException {
	validateString(key);
	validateString(value);
	outStream.println(key + "\t" + value);
	long time = System.currentTimeMillis();
	recordsWritten += 1;
	if (recordsBeforeSubmit > 0 && recordsWritten > recordsBeforeSubmit) {
	    try {
		submit();
	    } catch (InterruptedException e) {
		log.error("Failed to submit data to queue. Will retry shortly.");
	    }
	}
	return time;
    }

    public synchronized void submit() throws IOException, InterruptedException {
	log.info(this.toString()  + " submitting file.");
	outStream.close();
	if (bufferedOutputStream != null)
	    bufferedOutputStream.close();
	fileOutputStream.close();

	File stagedFile = stageFile(bufferFile);
	uploader.queueFileForUpload(stagedFile, remoteLocation, stagedFile.getName());
	newBufferFile();
	lastSubmitTime = System.currentTimeMillis();
    }

    private static SimpleDateFormat fileDateFormat = new SimpleDateFormat("yyyy_MM_dd_'at'_HH_mm_ss_z");
    private String fileNameString() {
	String timeString = fileDateFormat.format(new Date(), new StringBuffer(), new FieldPosition(0)).toString();
	return namePrefix + "-" +timeString + "-" + UUID.randomUUID() + ".tsv";
    }

    OutputStream fileOutputStream = null;
    OutputStream bufferedOutputStream = null;
    private void newBufferFile() throws IOException {
	outStream = null;
	bufferFile = getFileFromLocalDir();
	recordsWritten = countLines(bufferFile);
	if (noMemoryBuffer) {
	    fileOutputStream = new FileOutputStream(bufferFile, true);
	    outStream = new PrintStream(fileOutputStream);
	} else {
	    fileOutputStream = new FileOutputStream(bufferFile, true);
	    bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
	    outStream = new PrintStream(bufferedOutputStream);
	}
    }

    private File getFileFromLocalDir() throws IOException {
	File[] oldFiles = dataPath().listFiles();
	if (oldFiles.length > 0) {
	    return oldFiles[0];
	} else {
	    return File.createTempFile(namePrefix, ".tsv", dataPath());
	}
    }

    private long countLines(File file) throws IOException {
	if (!file.exists())
	    return 0;
	long count = 0;
	FileInputStream f = new FileInputStream(file);
	BufferedReader r = new BufferedReader(new InputStreamReader(f));
	String line = r.readLine();
	while (line != null) {
	    count += 1;
	    line = r.readLine();
	}
	return count;
    }

    public static class StreamHandlerException extends Exception {
	StreamHandlerException(String s) {
	    super(s);
	}
    };

    private File stagingPath() {
	return localStagingPath;
    }

    private File dataPath() {
	return localDataPath;
    }


    private File stageFile(File inFile) throws IOException {
	File stagedFile = new File(stagingPath(), fileNameString());
	if (!inFile.renameTo(stagedFile)) {
	    log.error("Unable to move file " + inFile + " to staging area " + stagedFile + ". Data may NOT be committed to the database.");
	    throw new IOException("Unable to move file " + inFile + " to staging area " + stagedFile + ".");
	}
	log.debug("Successfully copied file " + inFile + " to " + stagedFile);
	return stagedFile;
    }

    private static String validateString(String s) throws StreamHandlerException {
	if (s.indexOf("\n") != -1)
	    throw new StreamHandlerException("String contained newline at " + s.indexOf("\n"));

	if ((s.indexOf("\t") != -1))
	    throw new StreamHandlerException("String contained tab at " + s.indexOf("\t"));
	return s;
    }

    public synchronized void setRecordsBeforeSubmit(long records) {
	recordsBeforeSubmit = records;
    }

    //Set submit interval, in seconds.
    public synchronized void setSubmitInterval(long mySubmitInterval) {
	if (mySubmitInterval == submitInterval)
	    return;

	submitInterval = 1000*mySubmitInterval; //Convert from seconds to milliseconds

	if (submitInterval > 0 && submitHeartbeat == null) {
	    submitHeartbeat = new Thread(new SubmitHeartbeat());
	    submitHeartbeat.start();
	}
	if (submitInterval == -1 && submitHeartbeat != null) {
	    submitHeartbeat.interrupt();
	}
    }

    private Thread submitHeartbeat = null;

    private class SubmitHeartbeat implements Runnable {
	public void run() {
	    log.info("Starting heartbeat thread. " + toString() + " will submit data (when available) every " + submitInterval + "ms.");
	    try {
		while (true) {
		    sleepUntilNextSubmit();
		    if (recordsWritten > 0) {
			submit();
			log.info(toString() + " submitting data, triggered by time.");
		    }
		    else {
			lastSubmitTime = System.currentTimeMillis();
			log.info(toString() + " submitting data, triggered by time. Skipping upload since no data to submit.");
		    }
		}
	    } catch (InterruptedException e) {
		return;
	    } catch (IOException e) {
		log.error(toString() + " failed to submit data. Will retry on next heartbeat.");
	    }
	}

	private void sleepUntilNextSubmit() throws InterruptedException {
	    long time = System.currentTimeMillis();
	    long nextSubmit = lastSubmitTime + submitInterval;
	    if (nextSubmit > time)
		Thread.sleep(nextSubmit - time);
	}
    }

    public static void main(String[] args) throws IOException, StreamHandlerException, InterruptedException {
	FileUploader r = new FileUploader(new File("/tmp/staging"));

	RemoteLocation loc = new RemoteLocation("baz", args[0]);
	StreamHandler s = new StreamHandler("baz", r, "bazrecord", true, new File("/tmp/maryjane"), true, loc);
	s.setSubmitInterval(5000);
	s.setRecordsBeforeSubmit(4000);
	for (int i=0;i<10;i++) {
	    for (int j=0;j<500;j++) {
		s.addRecord(i + "," + j, UUID.randomUUID().toString());
	    }
	}
	System.out.println("Submitted records...");
	Thread.sleep(1000);
	System.out.println("Submitting more...");
	for (int i=0;i<10;i++) {
	    for (int j=0;j<500;j++) {
		s.addRecord(i + "," + j, UUID.randomUUID().toString());
	    }
	}
	System.out.println("Finished submitting...");
    }

}