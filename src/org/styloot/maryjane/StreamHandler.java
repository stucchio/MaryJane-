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

    private Path remotePath;
    private String namePrefix;
    private boolean useCompression;
    private String name;
    private final File localPath;
    private boolean noMemoryBuffer = false;
    private File bufferFile;
    private PrintStream outStream;

    public long recordLimitBeforeFlush = -1;

    private static long OFFER_TIMEOUT = 500;

    public StreamHandler(String myName, Path myRemotePath, String myPrefix, boolean compress, File localDir, boolean noBuffer) throws IOException {
	name = myName;
        remotePath = myRemotePath;
        namePrefix = myPrefix;
        useCompression = compress;
	noMemoryBuffer = noBuffer;

        localPath = new File(localDir, namePrefix);
        localPath.mkdirs();

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

    public synchronized void flush() throws IOException {
	String remoteFileNameString = fileNameString();
	Path pathToFlush = new Path(remotePath, remoteFileNameString);
	log.info("Streamhandler " + name + " flushing to " + pathToFlush);
	outStream.close();
	if (bufferedOutputStream != null)
	    bufferedOutputStream.close();
	fileOutputStream.close();
	FileSystem remoteFileSystem = remotePath.getFileSystem(new Configuration());

	remoteFileSystem.moveFromLocalFile(new Path(bufferFile.getAbsolutePath()), pathToFlush);
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
	File[] oldFiles = localPath.listFiles();
	if (oldFiles.length > 0) {
	    recordsWritten = countLines(oldFiles[0]);
	    return oldFiles[0];
	} else {
	    return File.createTempFile(namePrefix, ".tsv", localPath);
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

    private static String validateString(String s) throws StreamHandlerException {
	if (s.indexOf("\n") != -1)
	    throw new StreamHandlerException("String contained newline at " + s.indexOf("\n"));

	if ((s.indexOf("\t") != -1))
	    throw new StreamHandlerException("String contained tab at " + s.indexOf("\t"));
	return s;
    }

    public static void main(String[] args) throws IOException, StreamHandlerException {
	StreamHandler s = new StreamHandler("baz", new Path("s3://ID:SECRETKEY@BUCKET/"), "bazrecord", true, new File("/tmp/maryjane"), true);
	s.addRecord("foo", "bar");
	s.addRecord("biz", "baz");
	s.flush();
    }
}