<div id="container" align="center">
  <img
    src="http://tellapart.com/wp-content/uploads/2012/06/taba-kanji.gif"
    alt="Taba" />
</div>

Introduction
====================

Taba is a service for aggregating instrumentation events from large distributed
systems in near real-time. It was built to handle high throughput and scale
easily.

Check out an overview of Taba's architecture on the TellApart Eng Blog:
[http://tellaparteng.tumblr.com/post/49814523799/taba-low-latency-event-aggregation
](http://tellaparteng.tumblr.com/post/49814523799/taba-low-latency-event-aggregation)

Example
====================

Taba helps you instrument your services and provide a near real-time view into
what's happening across a large cluster. For example, you could use it to track
the winning bid price for a certain type of bid:

    from taba import client

    ...

    client.Counter('bids_won', 1)
    client.Counter('winning_bid_price', wincpm)

When those Events reach the Taba Server and are aggregated, they produce an
output like the following:

    $ taba-cli agg winning_bid_price
    winning_bid_price: {
        "1m": {"count": 436, "total": 571.64},
        "10m": {"count": 5285, "total": 6884.57},
        "1h": {"count": 34265, "total": 44175.47},
        "1d": {"count": 569787, "total": 744423.87},
        "pct": [0.09, 0.47, 2.19, 3.55, 4.37, 14.09, 17.59]}

There are many other input data types and aggregation methods available. See the
Types and Handlers documentation.

Overview
====================

A Taba deployment consists of 6 layers, each horizontally scalable. These
layers are:

- Taba Client code integrated into applications
- Taba Agent process running locally to the application servers
- Taba Server processes in the frontend ('fe') role
- Taba Server processes in the backend ('be') role
- Redis sentinel processes
- Redis database processes

The Taba Client is integrated into the application it is instrumenting, and
exposes an API for recording events to different counter types. The Python
distribution includes a default Client implementation based on threads, and
a Gevent engine. There is also a Java Client available
([https://github.com/tellapart/taba-java-client]
(https://github.com/tellapart/taba-java-client))

The Client typically sends events to a Taba Agent process running on the same
server. While the Client will only forward events on a best-effort basis, the
Agent provides more robust buffering and failure recovery. It is also
significantly smarter about load balancing.

The will forward events to one of the Taba Server end-points it has been
configured to connect to. Any Server in the cluster can receive any set of
events.

The Taba Server processes are split into two groups: frontend ('fe') and backend
('be'). These roles are configured when the process starts. Assigning a process
a 'fe' role has no effect on its operation -- it is simply a marker that the
process will use to advertise its role. (The intention it to allow a
load-balancer to use that indicator to route traffic to just the 'fe'
processes). Assigning the 'be' role will configure the process to launch a
background worker that processes queued events. There must be at least one 'be'
Server process in the cluster.

A Server process can be assigned both 'fe' and 'be' roles. For small clusters,
this will work well. However, once a cluster becomes large enough to require
multiple Server processes, separating 'fe' and 'be' processes will perform
better.

There is a third role 'ctrl', which essentially marks a Server process as
neither 'fe' nor 'be'. This is useful for maintaining a separate set of
processes for querying.

The Taba Server uses a group of Redis databases and Sentinels. Having at least
one Sentinel is a requirement, as it is used for service discovery of the
individual database processes. Sharding across the databases is accomplished by
splitting the key-space into a large number of virtual buckets, and assigning
ranges of buckets to each process.

Installing Taba
====================

Taba was designed to run on Python 2.6/2.7. It has the following Python package
dependencies, which should be installed automatically:

- gevent (>= 0.13.1)
- python-cjson (>= 1.0.5)
- cython (>= 0.13)
- redis (>= 2.9)
- requests (>= 1.2.0)

Additionally, building the Python dependencies requires the following. These
dependencies are _not_ installed automatically:

- gcc
- make
- python-dev
- libevent-dev

The latest stable release can be installed from PyPi:

    pip install taba

Or Taba can be installed directly from the repository:

    git clone https://github.com/tellapart/taba.git
    cd taba
    python setup.py install

Installing Redis
====================

The Taba Server is uses a group of Redis instances with Sentinels as its
database. It requires at least Redis 2.8. For details about installing Redis,
please visit the [Redis Downloads page](http://redis.io/download)

Deploying Taba
====================

There are many ways to deploy Taba, depending on the use case. See
`examples/EXAMPLES` for pointers on how to get started.

About
====================

Taba is a project at TellApart led by
[Kevin Ballard](https://github.com/kevinballard) to create a reliable, high
performance platform for real-time monitoring. It is used to monitor over
30,000 Tabs, consuming nearly 10,000,000 Events per second, and an average
latency of under 15s.

Any questions or comments can be forwarded to 
[taba@tellapart.com](mailto:taba@tellapart.com)
