package org.styloot.maryjane;

import org.styloot.maryjane.jsonsimple.*;

import org.apache.hadoop.fs.*;

import java.util.*;
import java.io.*;

public class MaryJaneWriter {
    File localDir;
    Map<String,StreamHandler> streams = new HashMap<String,StreamHandler>();

    public MaryJaneWriter(File myLocalDir) throws IOException {
	localDir = myLocalDir;
	if (!localDir.exists()) {
	    throw new IOException("Local directory " + myLocalDir + " does not exist.");
	}
    }



}