========
MaryJane
========

Maryjane serves a very simple purpose - putting data into Hadoop.


Dependencies
============

Hadoop 0.20 (may work with earlier versions)
log4j (Usually comes with Hadoop)
Apache Thrift

Installation
============

The server installation is easy:

    $ ./build.sh
    $ cp build/maryjane.jar SOMEPLACE_IN_CLASSPATH

The python client installation is similarly easy:

    $ mv build/python-maryjane.tgz /tmp
    $ cd /tmp
    $ tar -xvzf python-maryjane.tgz
    $ mv maryjane SOMEPLACE_IN_PYTHONPATH

Usage
=====

Maryjane is run as follows:

    $ java org.styloot.maryjane.thriftserver.Server PORT LOCALDIR PATH_TO_CONFIGURATION

The configuration is a JSON file stored on the remote filesystem. So, for example, suppose we have the following config file at hdfs://namenode/maryjane/config.json :

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

This will create two *streams* - one named "mary" and one named "jane". Data can be submitted to them via the addRecord(streamname, key, value) method in the thrift interface.

Suppose the command addRecord("mary", "foo", "bar") is called. This will result (eventually) in the following:

    $ hadoop fs -ls hdfs://namenode/maryjane/mary
    ...
    -rwxrwxrwx   1       2304 2011-04-16 20:22 /maryjane/mary/mary-2011_04_16_at_20_22_34_EDT-d8accee2-a471-459a-97b4-19dfe48fb4cf.tsv
    ...
    $ hadoop fs -cat hdfs://namenode/maryjane/mary-2011_04_16_at_20_22_34_EDT-d8accee2-a471-459a-97b4-19dfe48fb4cf.tsv
    ...
    foo		     bar
    ...

The timestamp on the file has nothing to do with the time the record was submitted, but is merely the time the file was uploaded.



The parameter "submit_interval" is the number of seconds between submissions. By default, this is 60*60 = 1 hour. I.e., data will be submitted from MaryJane to HDFS every hour. There are other options, as described in the list of options below.

List of options
---------------

* submit_interval (int) - number of seconds between submissions.

For example, if submit_interval = 3600, then records will be uploaded to hadoop every hour.

* max_records (int) - number of records between submissions. If max_records = 5000, then every time the number of records exceeds 5000, the file will be submitted to hadoop. If both submit_interval and max_records are specified, then both will apply. I.e., a file will be uploaded to HDFS every 5000 records or 1 hour, whichever occurs earliest (assuming submit_interval=3600 and max_records=5000).

* max_file_size (int) - when the size of the file exceeds this number (in bytes), the file will be submitted. This number should only be interpreted *approximately* - if you set max_file_size to 1048576, there is a good chance the file that is submitted will be somewhat larger than 1048576.

* compress (boolean) - whether to gzip the files before submission. False by default.

* no_buffer (boolean) - whether or not to write every record to the *local* disk. If no_buffer = false, then records may be lost if the server crashes. If no_buffer is set to true, performance will be reduced. False by default.

* flush_interval (int) - data will be synced to the disk every flush_interval milliseconds. This has no effect if no_buffer is set to true. Default = 5000 (5 seconds).

Client
------

See the thrift file for reference. An example in python:

    from thrift import Thrift
    from thrift.transport import TSocket
    from thrift.transport import TTransport
    from thrift.protocol import TBinaryProtocol

    from maryjane.MaryJane import Client

    transport = TSocket.TSocket('localhost', 10289)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = Client(protocol)
    transport.open()

    client.addRecord("mary", "foo", "bar")
    client.addRecord("jane", "fizz", "buzz")

    client.sync("jane") # This forces MaryJane to submit all data in the 'jane' stream to Hadoop.


Frequently Asked Questions
==========================

* What about languages other than Java and Python?

The interface is defined by Apache Thrift, so this should be easy. Just add an appropriate namespace to the thrift file, and run the command:

    $ thrift --gen YOUR_LANG maryjane.thrift

* Why is it called MaryJane?

Most Styloot projects have fashion related names.

http://en.wikipedia.org/wiki/Mary_Jane_(shoe)

http://www.google.com/images?um=1&hl=en&tbm=isch&sa=X&ei=1fKqTfWUPOXUiAK9xIGODw&ved=0CDMQBSgA&q=mary+jane+shoe&spell=1&biw=1198&bih=675

* How does MaryJane compare to Flume?

MaryJane is a lot simpler. Compare this readme to Flume's manual, or compare code size. Maryjane is about 600 lines of code, 2800 if you include the size of the jsonsimple library which is embedded in it.

Unlike MaryJane, Flume has reliability guarantees. It would be a very bad idea to use MaryJane if it is vital that all records enter the database. Cloudera also supports Flume, which can be handy.
