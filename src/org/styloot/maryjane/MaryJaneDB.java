package org.styloot.maryjane;

import org.styloot.maryjane.jsonsimple.*;

import org.apache.hadoop.fs.*;

import java.util.*;
import java.io.*;

public class MaryJaneDB {
    File localDir;

    public MaryJaneDB(JSONObject conf, File myLocalDir) throws IOException {
	localDir = myLocalDir;
	if (!localDir.exists()) {
	    throw new IOException("Local directory " + myLocalDir + " does not exist.");
	}

    }

    Map<String,StreamHandler> streams = new HashMap<String,StreamHandler>();


}