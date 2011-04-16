package org.styloot.maryjane;

import org.apache.hadoop.fs.*;

public class RemoteLocation {
    public final String name;
    public final Path remotePath;

    public RemoteLocation(String nm, Path rp) {
        name = nm;
        remotePath = rp;
    }

    public RemoteLocation(String nm, String rp) {
        name = nm;
        remotePath = new Path(rp);
    }
}