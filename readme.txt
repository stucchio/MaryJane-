MaryJane
========

A simple recordwriter for Hadoop. Like Flume, but much simpler.

Dependencies
------------
Hadoop 0.20 (may work with earlier versions)
log4j (Usually comes with Hadoop)
Apache Thrift

Installation
------------

    ./build.sh
    cp maryjane.jar SOMEPLACE_IN_CLASSPATH

Usage
-----
Maryjane is run as follows:

    $ java org.styloot.maryjane.thriftserver.Server PORT LOCALDIR PATH_TO_CONFIGURATION

The configuration is a JSON file stored on the remote filesystem. So, for example, suppose we
have the following config file at hdfs://namenode/maryjane/config.json ::
    {
      "streams" : {
          "mary" : { "path" : "maryjane/mary",
    		 "prefix" : "maryprefix",
    		 "submit_interval" : 30,
    	       },
          "jane" : { "path" : "maryjane/jane",
    		 "prefix" : "jane",
    		 "max_records" : 50,
    	       },
      }
    }

This will create two *streams* - one named "mary" and one named "jane". Data can be submitted to them via the
addRecord(streamname, key, value) method in the thrift interface.

So addRecord("mary", "foo", "bar") will eventually result in the line:

    foo		     bar

being located in some file in the folder hdfs://namenode/maryjane/mary

The file will have the prefix "maryprefix", and have a name something along the lines of:

    jane-2011_04_16_at_20_22_32_EDT-7ae76d78-4ff5-4cfb-9157-06db77854af6.tsv

(The timestamp on the file has nothing to do with the time the record was submitted, but is merely the time the file was uploaded.)

The parameter "submit_interval" is the number of seconds between submissions. By default, this is 60*60 = 1 hour. I.e., data will
be submitted from MaryJane to HDFS every hour. max_records specifies the number of records required before submission - that is, if max_records is 5000, then once 5000 records have been submitted, a file will be uploaded to HDFS.

If both are specified, then both will apply (i.e., a file will be uploaded to HDFS every 5000 records or 1 hour, whichever occurs earliest.)

Client
~~~~~~

See the file example/python/example.py to see the usage of the python interface.



