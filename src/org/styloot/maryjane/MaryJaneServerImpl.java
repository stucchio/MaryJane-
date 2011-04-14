package org.styloot.maryjane;

import java.util.*;
import org.apache.thrift.*;
import tserver.gen.*;

class MaryJaneServerImpl implements TimeServer.Iface
{
    @Override
    public long time() throws TException
    {
    long time = System.currentTimeMillis();
    System.out.println("Current time : "+time);
    return time;
  }

  public String date() throws TException
  {
    Calendar now = Calendar.getInstance();
    String dt = now.get(Calendar.YEAR)+"-"+(now.get(Calendar.MONTH)+1)+"-"+now.get(Calendar.DAY_OF_MONTH)+" "+now.get(Calendar.HOUR_OF_DAY)+":"+now.get(Calendar.MINUTE)+":"+now.get(Calendar.SECOND);
    System.out.println(" date : "+dt);
    return dt;
  }

  public long multiply(int num1, int num2) throws TException
  {
    System.out.println("Got : "+num1+", "+num2);
    return num1*num2;
  }
}