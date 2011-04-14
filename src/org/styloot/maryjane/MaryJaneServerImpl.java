package org.styloot.maryjane;

import java.util.*;
import org.apache.thrift.*;

import org.styloot.maryjane.gen.*;

class MaryJaneServerImpl implements MaryJane.Iface
{
    public MaryJaneServerImpl() {
    }

    @Override
    public long addRecord(String streamname, String key, String value) throws TException {
        long time = System.currentTimeMillis();
        System.out.println("Current time : "+time);
        return time;
    }

    @Override
    public long sync(String streamname) throws TException {
	return 0;
    }

}