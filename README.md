<div id="container" align="center">
  <img
    src="http://tellapart.com/wp-content/uploads/2012/06/taba-kanji.gif"
    alt="Taba" />
</div>

Introduction
====================

Taba is a service for aggregating events from a distributed system in near
real-time. It was built to handle high throughput and scale easily.

Check out an overview of Taba's architecture on the TellApart Eng Blog:
[http://tellapart.com/taba-low-latency-event-aggragation
](http://tellapart.com/taba-low-latency-event-aggragation)

Also, take a look at the Overview wiki page:
[http://github.com/tellapart/taba/wiki/Overview
](http://github.com/tellapart/taba/wiki/Overview)

Example
====================

There are many use-cases for Taba. A common one is to track the aggregate
execution frequency and duration of some operation. For example, the following
code could be added around a database put operation:

    start = time.time()
    
    ...[perform database put]...
    
    elapsed = time.time() - start
    taba_client.RecordValue('database_put_time', (elapsed * 1000,), "CounterGroup")

When those Events reach the Taba Server and are aggregated, they produce an
output like:

    database_put_time_1m   156   13926.0
    database_put_time_10m   2500   253961.0
    database_put_time_1d   295677   23803205.0
    database_put_time_1h   15259   1695586.0
    database_put_time_pct   448662   32358553.0   [(0.25, 44.9), (0.50, 56.1),
    (0.75, 76.6), (0.90, 109.3), (0.95, 156.9), (0.99, 399.3)]

This means that, across all Clients producing this Event, over the last minute
for example, 156 put operations occurred totaling 13926 ms. Over the history of
the "`database_put_time`" Tab for example, the 50th percentile is 56.1 ms.

There are many other input data types and aggregation methods available. See the
Types and Handlers documentation.

Installing Taba
====================

Taba was designed to run on Python 2.6 (limited support has been added for
Python 2.7, but it is not officially supported). Taba has the following
dependencies:

-  gevent (0.13.1)
-  cjson (1.0.5)
-  cython (0.13)

To run the Taba unit tests, the following are also required:

- unittest2
- mox

Finally, to build the dependencies, the following are needed:

- gcc
- make
- python-dev
- libevent-dev

To install the dependencies on Ubuntu (12.04), for example:

    sudo apt-get install gcc
    sudo apt-get install make
    sudo apt-get install python-dev
    sudo apt-get install libevent-dev
    sudo apt-get install python-setuptools
    
    sudo easy_install gevent
    sudo easy_install python-cjson
    sudo easy_install cython
    
    sudo easy_install unittest2
    sudo easy_install mox

To install Taba, simply download and extract the source repository. (Deployment
tools are in the works for later releases). Execute `run_tests.sh` from the root
to make sure everything is installed correctly.

Installing Redis
====================

The Taba Server is designed to use Redis 2.4 as its datastore (a single process
Taba server can run with an in-process memory mode, but this is not recommended
for actual deployments.)

To install Redis 2.4:

    # Download and extract Redis 2.4
    wget http://redis.googlecode.com/files/redis-2.4.14.tar.gz
    tar -zxvf redis-2.4.14.tar.gz
    cd redis-2.4.14/
    
    # Build
    make

    # Optionally run the tests
    sudo apt-get install tcl8.5
    make test
    
    # Install
    sudo make install

Deploying Taba
====================

There are many ways to deploy Taba, depending on the use case. See
`examples/EXAMPLES` for pointers on how to get started.

About
====================

Taba is a project at TellApart led by
[Kevin Ballard](https://github.com/kevinballard) to create a reliable, high
performance platform for real-time monitoring. It is used to monitor over
10,000 Tabs, consuming nearly 15,000,000 Events per minute, and an average
latency of under 30s.

Any questions or comments can be forwarded to 
[taba@tellapart.com](mailto:taba@tellapart.com)
