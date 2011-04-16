package org.styloot.maryjane.thriftserver;

import java.util.*;
import java.io.*;
import org.apache.thrift.*;

import org.styloot.maryjane.gen.*;
import org.styloot.maryjane.*;

class MaryJaneServerImpl implements MaryJane.Iface
{
    public MaryJaneServerImpl(MaryJaneWriter myWriter) {
	writer = myWriter;
    }

    private MaryJaneWriter writer;

    @Override
    public long addRecord(String streamname, String key, String value) throws TException, MaryJaneException, MaryJaneException {
	try {
	    return writer.addRecord(streamname,key,value);
	} catch (IOException e) {
	    throw new MaryJaneException("IOException encountered." + e);
	} catch (IndexOutOfBoundsException e) {
	    throw new MaryJaneException("No such stream '" + streamname + "'");
	}
    }

    @Override
    public long sync(String streamname) throws TException, MaryJaneException, MaryJaneException {
	try {
	    return writer.sync(streamname);
	} catch (IOException e) {
	    throw new MaryJaneException("IOException encountered." + e);
	} catch (InterruptedException e) {
	    throw new MaryJaneException("Server error - thread interrupted." + e);
	} catch (IndexOutOfBoundsException e) {
	    throw new MaryJaneException("No such stream '" + streamname + "'");
	}
    }

}