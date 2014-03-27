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

Also, take a look at the Overview wiki page:
[http://github.com/tellapart/taba/wiki/Overview
](http://github.com/tellapart/taba/wiki/Overview)

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

The Taba Server is designed to use a group of Redis instances (> 2.4) as its
database. For details about installing Redis, please visit the [Redis Downloads
page](http://redis.io/download)

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
