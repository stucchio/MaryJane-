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

    private RecordUploader uploader;
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

    public long recordLimitBeforeFlush = -1;

    private static long OFFER_TIMEOUT = 500;

    public StreamHandler(String myName, RecordUploader myUploader, String myPrefix, boolean compress, File localDir, boolean noBuffer, RemoteLocation myRemoteLocation) throws IOException {
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

    long recordsWritten = 0;
    public synchronized long addRecord(String key, String value) throws StreamHandlerException, IOException {
	validateString(key);
	validateString(value);
	outStream.println(key + "\t" + value);
	long time = System.currentTimeMillis();
	recordsWritten += 1;
	return time;
    }

    public synchronized void flush() throws IOException, InterruptedException {
	log.info("Streamhandler " + name + " flushing file.");
	outStream.close();
	if (bufferedOutputStream != null)
	    bufferedOutputStream.close();
	fileOutputStream.close();

	File stagedFile = stageFile(bufferFile);
	uploader.queueFileForUpload(stagedFile, remoteLocation, stagedFile.getName());
	newBufferFile();
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
	    recordsWritten = countLines(oldFiles[0]);
	    return oldFiles[0];
	} else {
	    return File.createTempFile(namePrefix, ".tsv", dataPath());
	}
    }

    private long countLines(File file) throws IOException {
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

    public static void main(String[] args) throws IOException, StreamHandlerException, InterruptedException {
	RecordUploader r = new RecordUploader(new File("/tmp/staging"));

	RemoteLocation loc = new RemoteLocation("baz", args[0]);
	StreamHandler s = new StreamHandler("baz", r, "bazrecord", true, new File("/tmp/maryjane"), true, loc);
	for (int i=0;i<10;i++) {
	    for (int j=0;j<500;j++) {
		s.addRecord(i + "," + j, UUID.randomUUID().toString());
	    }
	    s.flush();
	}
    }
}